"""Extiende demo_prefect.py con tres escenarios y salida CSV.

Reutiliza las tareas transformar y guardar de demo_prefect.py (que no cambia).

Uso: python demo_prefect_escenarios.py [normal|temporal|permanente]
Código de salida: 0 si el flujo termina en Completed, 1 si termina en Failed.
"""

import argparse
import csv
import os
import sys
from pathlib import Path

from prefect import flow, task
from prefect.runtime import flow_run, task_run

from demo_prefect import guardar, transformar

RESULTADOS = Path(__file__).resolve().parent / "resultados"
FALLOS = {"normal": 0, "temporal": 2, "permanente": float("inf")}


@task(name="extraer", retries=3, retry_delay_seconds=2, log_prints=True)
def extraer(escenario: str) -> list[int]:
    # run_count pertenece a esta ejecución de la tarea, así que cada flujo empieza en 1.
    n = task_run.run_count
    print(f"Extraer: intento {n}")
    if n <= FALLOS[escenario]:
        raise ConnectionError(f"Desconexión simulada (intento {n})")
    datos = [10, 20, 30]
    print(f"Extraer: éxito en el intento {n} -> {datos}")
    return datos


@task(log_prints=True)
def exportar_csv(datos: list[int], total: int, destino: Path) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporal = destino.with_suffix(".tmp")
    with temporal.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["concepto", "valor"])
        w.writerows((f"registro_{i}", v) for i, v in enumerate(datos, 1))
        w.writerow(["total", total])
    os.replace(temporal, destino)
    print(f"CSV: {destino.name} (total = {total})")


@flow(name="pipeline-etl-escenarios", log_prints=True)
def pipeline(escenario: str = "normal", carpeta: str | None = None) -> int:
    if escenario not in FALLOS:
        raise ValueError(f"Escenario desconocido: {escenario}")
    limpios = transformar(extraer(escenario))
    total = guardar(limpios)
    destino = Path(carpeta or RESULTADOS) / f"prefect_{escenario}_{flow_run.id}.csv"
    exportar_csv(limpios, total, destino)
    return total


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("escenario", nargs="?", choices=FALLOS, default="normal")
    estado = pipeline(p.parse_args().escenario, return_state=True)
    print(f"Estado final: {estado.type.value}")
    sys.exit(0 if estado.is_completed() else 1)
