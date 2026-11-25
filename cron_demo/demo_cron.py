#!/usr/bin/env python3
"""ETL convencional (sin Prefect) para ejecutarse desde Cron.

Subcomandos:
  etl          Proceso completo con UN solo intento (sin reintentos).
  extraer      Solo la extracción; lo invoca reintentos.sh en cada intento.
  transformar  Etapa dependiente de extraer.
  guardar      Etapa dependiente de transformar; escribe el CSV final.

Códigos de salida: 0 éxito, 1 error general, 2 argumentos inválidos,
75 (EX_TEMPFAIL) fallo de conexión simulado.
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
FALLOS = {"normal": 0, "temporal": 2, "permanente": float("inf")}
EX_TEMPFAIL = 75


def siguiente_intento(estado: Path | None) -> int:
    """Número de intento; se guarda en archivo porque cada intento es otro proceso."""
    if estado is None:
        return 1
    n = (int(estado.read_text()) if estado.exists() else 0) + 1
    estado.write_text(str(n))
    return n


def extraer(escenario: str, estado: Path | None = None) -> list[int]:
    n = siguiente_intento(estado)
    print(f"Extraer: intento {n} (pid {os.getpid()})", flush=True)
    if n <= FALLOS[escenario]:
        raise ConnectionError(f"Desconexión simulada (intento {n})")
    datos = [10, 20, 30]
    print(f"Extraer: éxito en el intento {n} -> {datos}", flush=True)
    return datos


def transformar(datos: list[int]) -> list[int]:
    resultado = [x * 2 for x in datos]
    print(f"Transformar: {datos} -> {resultado}", flush=True)
    return resultado


def guardar(datos: list[int], destino: Path) -> int:
    """Escribe el CSV de forma atómica: nunca queda un archivo a medias."""
    total = sum(datos)
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporal = destino.with_suffix(f".{os.getpid()}.tmp")
    with temporal.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["concepto", "valor"])
        w.writerows((f"registro_{i}", v) for i, v in enumerate(datos, 1))
        w.writerow(["total", total])
    os.replace(temporal, destino)
    print(f"Guardar: {destino.name} (total = {total})", flush=True)
    return total


def main() -> int:
    p = argparse.ArgumentParser(description="ETL convencional para Cron")
    sub = p.add_subparsers(dest="etapa", required=True)

    s = sub.add_parser("etl", help="proceso completo, un solo intento")
    s.add_argument("--escenario", choices=FALLOS, default="normal")
    s.add_argument("--salida", type=Path, help="CSV de destino")

    s = sub.add_parser("extraer")
    s.add_argument("--escenario", choices=FALLOS, default="normal")
    s.add_argument("--estado", type=Path, required=True, help="contador de intentos")
    s.add_argument("--salida", type=Path, required=True)

    for nombre in ("transformar", "guardar"):
        s = sub.add_parser(nombre)
        s.add_argument("--entrada", type=Path, required=True)
        s.add_argument("--salida", type=Path, required=True)

    a = p.parse_args()
    try:
        if a.etapa == "etl":
            destino = a.salida or RAIZ / "resultados" / (
                f"cron_{a.escenario}_{datetime.now():%Y%m%d_%H%M%S}_{os.getpid()}.csv"
            )
            guardar(transformar(extraer(a.escenario)), destino)
        elif a.etapa == "extraer":
            a.salida.write_text(json.dumps(extraer(a.escenario, a.estado)))
        elif a.etapa == "transformar":
            a.salida.write_text(json.dumps(transformar(json.loads(a.entrada.read_text()))))
        else:
            guardar(json.loads(a.entrada.read_text()), a.salida)
    except ConnectionError as e:
        print(f"ERROR ConnectionError: {e}", file=sys.stderr)
        return EX_TEMPFAIL
    return 0


if __name__ == "__main__":
    # Con Cron (entorno mínimo) o Windows la codificación puede no ser UTF-8.
    for flujo in (sys.stdout, sys.stderr):
        flujo.reconfigure(encoding="utf-8")
    sys.exit(main())
