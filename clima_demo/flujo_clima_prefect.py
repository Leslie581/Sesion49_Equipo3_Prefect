#!/usr/bin/env python3
"""ETL de clima (Open-Meteo) orquestado con Prefect. Reutiliza etl_clima.py.

Solo se reintentan los fallos transitorios (red, 429, 5xx, desconexión simulada).

Uso:
  python clima_demo/flujo_clima_prefect.py --escenario temporal      # una ejecución
  python clima_demo/flujo_clima_prefect.py --servir                  # diario 12:00 America/Mexico_City
Códigos de salida: 0 Completed, 1 Failed.
"""

import argparse
import sys
from pathlib import Path

from prefect import flow, task
from prefect.runtime import task_run
from prefect.schedules import Cron

sys.path.insert(0, str(Path(__file__).resolve().parent))
import etl_clima as etl  # noqa: E402

CRON_DIARIO = "0 12 * * *"


def es_transitorio(tarea, ejecucion, estado) -> bool:
    """retry_condition_fn: True solo si la excepción es reintentable."""
    try:
        estado.result()
    except (ConnectionError, etl.ErrorTransitorio):
        return True
    except Exception:
        return False
    return False


@task(retries=3, retry_delay_seconds=5, retry_condition_fn=es_transitorio, log_prints=True)
def extraer(escenario: str) -> list[dict]:
    print(f"Extraer Open-Meteo: intento {task_run.run_count} (escenario={escenario})")
    crudo = etl.extraer(escenario, task_run.run_count)
    print(f"Extraer: {len(crudo)} ubicaciones recibidas")
    return crudo


@task(log_prints=True)
def transformar(crudo: list[dict]) -> list[dict]:
    filas, descartes = etl.transformar(crudo)
    for d in descartes:
        print(f"Transformar: descartada -> {d}")
    for f in filas:
        print(f"  {f['fecha_hora_local']} {f['ciudad']}: {f['temperatura']} {f['temperatura_unidad']}, "
              f"{f['humedad']} {f['humedad_unidad']}, {f['precipitacion']} {f['precipitacion_unidad']}, "
              f"{f['viento']} {f['viento_unidad']}")
    return filas


@task(log_prints=True)
def guardar(filas: list[dict], salida: str) -> int:
    nuevas, total = etl.guardar(filas, Path(salida))
    print(f"Guardar: {nuevas} filas nuevas, {len(filas) - nuevas} duplicadas omitidas, "
          f"{total} en {Path(salida).name}")
    return nuevas


@flow(name="etl-clima-colima", log_prints=True)
def flujo_clima(escenario: str = "normal", salida: str = str(etl.CSV_PREDETERMINADO)) -> int:
    return guardar(transformar(extraer(escenario)), salida)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="ETL de clima Open-Meteo con Prefect")
    p.add_argument("--escenario", choices=["normal", "temporal", "permanente"], default="normal")
    p.add_argument("--salida", default=str(etl.CSV_PREDETERMINADO))
    p.add_argument("--servir", action="store_true",
                   help=f"programa '{CRON_DIARIO}' en {etl.ZONA_HORARIA} (requiere servidor Prefect)")
    p.add_argument("--cron", default=CRON_DIARIO, help="solo con --servir (útil para pruebas)")
    a = p.parse_args()
    if a.servir:
        flujo_clima.serve(name="clima-diario-12pm",
                          schedules=[Cron(a.cron, timezone=etl.ZONA_HORARIA)],
                          parameters={"escenario": a.escenario, "salida": a.salida})
        sys.exit(0)
    estado = flujo_clima(a.escenario, a.salida, return_state=True)
    print(f"Estado final: {estado.type.value}")
    sys.exit(0 if estado.is_completed() else 1)
