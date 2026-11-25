"""Ejecuta el flujo cada 60 segundos mientras este proceso siga abierto.

Requiere un servidor (prefect server start) y PREFECT_API_URL configurada.
Se detiene con Ctrl+C. Para medianoche: cron="0 0 * * *" en lugar de interval.

Uso: python programacion_recurrente.py [normal|temporal|permanente]
"""

import argparse

from demo_prefect_escenarios import FALLOS, pipeline

if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("escenario", nargs="?", choices=FALLOS, default="temporal")
    pipeline.serve(
        name="etl-cada-60-segundos",
        interval=60,
        parameters={"escenario": p.parse_args().escenario},
        limit=1,  # máximo una ejecución simultánea en este proceso
    )
