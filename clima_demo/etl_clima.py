#!/usr/bin/env python3
"""ETL de clima (Open-Meteo) compartido por Cron y Prefect. Solo usa la biblioteca estándar.

Extrae las condiciones actuales de Colima, Villa de Álvarez y Manzanillo en UNA petición,
valida y limpia los valores y los agrega a un CSV histórico sin duplicados
(clave: ciudad + fecha_hora_local).

Escenarios de fallo (simulados ANTES de la petición real, que siempre es real):
  normal      sin simulación
  temporal    los intentos 1 y 2 fallan con ConnectionError; el 3 consulta la API real
  permanente  todos los intentos fallan con ConnectionError

Uso en Cron (un intento; los reintentos los hace cron_clima.sh):
  python3 clima_demo/etl_clima.py --escenario temporal --intento 1
Códigos de salida: 0 éxito, 75 fallo transitorio (reintentable), 2 error no reintentable.
"""

import argparse
import csv
import json
import math
import os
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
CSV_PREDETERMINADO = RAIZ / "resultados" / "clima_historico.csv"
URL_API = "https://api.open-meteo.com/v1/forecast"
FUENTE = "Open-Meteo (api.open-meteo.com/v1/forecast, modelo best_match)"
ZONA_HORARIA = "America/Mexico_City"  # zona IANA que Open-Meteo usa para "time"
CIUDADES = {  # coordenadas aproximadas del centro de cada ciudad
    "Colima": (19.2433, -103.7250),
    "Villa de Álvarez": (19.2672, -103.7378),
    "Manzanillo": (19.0522, -104.3158),
}
# variable de la API -> (columna del CSV, mínimo válido, máximo válido)
VARIABLES = {
    "temperature_2m": ("temperatura", -30.0, 55.0),
    "relative_humidity_2m": ("humedad", 0.0, 100.0),
    "precipitation": ("precipitacion", 0.0, 500.0),
    "wind_speed_10m": ("viento", 0.0, 300.0),
}
COLUMNAS = (["fecha_hora_local", "zona_horaria", "ciudad", "latitud", "longitud"]
            + [c for v in VARIABLES.values() for c in (v[0], f"{v[0]}_unidad")]
            + ["fuente", "consultado_utc"])
EXIT_TRANSITORIO, EXIT_PERMANENTE = 75, 2
INTENTOS_FALLIDOS_TEMPORAL = 2


class ErrorTransitorio(Exception):
    """Fallo de red, tiempo de espera, HTTP 429 o 5xx: tiene sentido reintentar."""


class ErrorPermanente(Exception):
    """Petición o datos inválidos (HTTP 4xx, JSON inesperado): reintentar no sirve."""


def simular_fallo(escenario: str, intento: int) -> None:
    if escenario == "permanente" or (escenario == "temporal" and intento <= INTENTOS_FALLIDOS_TEMPORAL):
        raise ConnectionError(f"Desconexión simulada (escenario={escenario}, intento {intento})")
    if escenario not in ("normal", "temporal", "permanente"):
        raise ValueError(f"Escenario desconocido: {escenario}")


def construir_url() -> str:
    lat, lon = zip(*CIUDADES.values())
    return URL_API + "?" + urllib.parse.urlencode({
        "latitude": ",".join(map(str, lat)), "longitude": ",".join(map(str, lon)),
        "current": ",".join(VARIABLES), "timezone": ZONA_HORARIA,
        "wind_speed_unit": "kmh", "temperature_unit": "celsius", "precipitation_unit": "mm",
    })


def extraer(escenario: str = "normal", intento: int = 1, timeout: float = 15) -> list[dict]:
    """Devuelve la respuesta cruda de Open-Meteo (una entrada por ciudad)."""
    simular_fallo(escenario, intento)
    peticion = urllib.request.Request(construir_url(), headers={"User-Agent": "sesion49-etl-clima"})
    try:
        with urllib.request.urlopen(peticion, timeout=timeout) as r:
            datos = json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 429 or e.code >= 500:
            raise ErrorTransitorio(f"HTTP {e.code} de Open-Meteo") from e
        raise ErrorPermanente(f"HTTP {e.code}: {e.read()[:200]!r}") from e
    except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
        raise ErrorTransitorio(f"Sin conexión con Open-Meteo: {e}") from e
    except json.JSONDecodeError as e:
        raise ErrorPermanente(f"Respuesta no es JSON: {e}") from e
    datos = datos if isinstance(datos, list) else [datos]
    if len(datos) != len(CIUDADES):
        raise ErrorPermanente(f"Se esperaban {len(CIUDADES)} ubicaciones y llegaron {len(datos)}")
    return datos


def transformar(crudo: list[dict], consultado_utc: str | None = None) -> tuple[list[dict], list[str]]:
    """Valida y limpia. Devuelve (filas válidas, motivos de descarte)."""
    consultado_utc = consultado_utc or datetime.now(timezone.utc).isoformat(timespec="seconds")
    filas, descartes = [], []
    for ciudad, item in zip(CIUDADES, crudo):
        actual, unidades = item.get("current") or {}, item.get("current_units") or {}
        try:
            fecha = datetime.fromisoformat(actual["time"]).strftime("%Y-%m-%dT%H:%M")
        except (KeyError, TypeError, ValueError):
            descartes.append(f"{ciudad}: fecha ausente o inválida"); continue
        fila = {"fecha_hora_local": fecha, "zona_horaria": item.get("timezone", ZONA_HORARIA),
                "ciudad": ciudad, "latitud": round(item.get("latitude", CIUDADES[ciudad][0]), 4),
                "longitud": round(item.get("longitude", CIUDADES[ciudad][1]), 4),
                "fuente": FUENTE, "consultado_utc": consultado_utc}
        motivo = None
        for var, (col, minimo, maximo) in VARIABLES.items():
            valor = actual.get(var)
            if not isinstance(valor, (int, float)) or isinstance(valor, bool) or not math.isfinite(valor):
                motivo = f"{ciudad}: {col} ausente o no numérico ({valor!r})"; break
            if not minimo <= valor <= maximo:
                motivo = f"{ciudad}: {col}={valor} fuera de [{minimo}, {maximo}]"; break
            fila[col], fila[f"{col}_unidad"] = round(float(valor), 1), str(unidades.get(var, "")).strip()
        if motivo:
            descartes.append(motivo)
        else:
            filas.append(fila)
    if not filas:
        raise ErrorPermanente("Ninguna fila superó la validación: " + "; ".join(descartes))
    return filas, descartes


def guardar(filas: list[dict], destino: Path = CSV_PREDETERMINADO) -> tuple[int, int]:
    """Agrega al histórico sin duplicar (ciudad, fecha_hora_local). Devuelve (nuevas, total)."""
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    existentes = []
    if destino.exists():
        with destino.open(newline="", encoding="utf-8") as f:
            existentes = list(csv.DictReader(f))
    vistas = {(f["ciudad"], f["fecha_hora_local"]) for f in existentes}
    nuevas = [f for f in filas if (f["ciudad"], f["fecha_hora_local"]) not in vistas]
    todas = sorted(existentes + nuevas, key=lambda f: (f["fecha_hora_local"], f["ciudad"]))
    fd, tmp = tempfile.mkstemp(dir=destino.parent, suffix=".tmp")
    with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:  # escritura atómica
        w = csv.DictWriter(f, fieldnames=COLUMNAS)
        w.writeheader(); w.writerows(todas)
    os.replace(tmp, destino)
    return len(nuevas), len(todas)


def main() -> int:
    p = argparse.ArgumentParser(description="ETL de clima Open-Meteo (un intento)")
    p.add_argument("--escenario", choices=["normal", "temporal", "permanente"], default="normal")
    p.add_argument("--intento", type=int, default=1)
    p.add_argument("--salida", type=Path, default=CSV_PREDETERMINADO)
    a = p.parse_args()
    print(f"Extraer: intento {a.intento} (escenario={a.escenario})", flush=True)
    try:
        crudo = extraer(a.escenario, a.intento)
    except (ConnectionError, ErrorTransitorio) as e:
        print(f"Extraer: fallo transitorio: {e}", file=sys.stderr); return EXIT_TRANSITORIO
    except ErrorPermanente as e:
        print(f"Extraer: error no reintentable: {e}", file=sys.stderr); return EXIT_PERMANENTE
    try:
        filas, descartes = transformar(crudo)
    except ErrorPermanente as e:
        print(f"Transformar: {e}", file=sys.stderr); return EXIT_PERMANENTE
    for d in descartes:
        print(f"Transformar: descartada -> {d}", file=sys.stderr)
    nuevas, total = guardar(filas, a.salida)
    for f in filas:
        print(f"  {f['fecha_hora_local']} {f['ciudad']}: {f['temperatura']} {f['temperatura_unidad']}, "
              f"{f['humedad']} {f['humedad_unidad']}, {f['precipitacion']} {f['precipitacion_unidad']}, "
              f"{f['viento']} {f['viento_unidad']}")
    print(f"Guardar: {nuevas} filas nuevas, {len(filas) - nuevas} duplicadas omitidas, "
          f"{total} en {a.salida.name}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
