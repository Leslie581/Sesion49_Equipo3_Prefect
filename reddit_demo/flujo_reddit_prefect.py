#!/usr/bin/env python3
"""Extracción de comentarios de Reddit orquestada con Prefect.

Reutiliza extractor_reddit.py. Solo los errores de red se reintentan: con credenciales
inválidas o una publicación inexistente, reintentar no sirve y el flujo falla de inmediato.

Uso (requiere Prefect, PRAW y las variables REDDIT_*):
  python reddit_demo/flujo_reddit_prefect.py --subreddit dataengineering --publicaciones 3
Códigos de salida: 0 Completed, 1 Failed, 2 configuración inválida.
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from prefect import flow, task
from prefect.runtime import task_run

sys.path.insert(0, str(Path(__file__).resolve().parent))
import extractor_reddit as er  # noqa: E402


def es_error_de_red(tarea, ejecucion, estado) -> bool:
    """retry_condition_fn: True solo si la excepción es transitoria."""
    try:
        estado.result()
    except er.errores_de_red():
        return True
    except Exception:
        return False
    return False


@task(retries=3, retry_delay_seconds=2, retry_condition_fn=es_error_de_red, log_prints=True)
def extraer_reddit(post: str | None, subreddit: str | None, publicaciones: int,
                   max_comentarios: int) -> list[dict]:
    print(f"Extraer Reddit: intento {task_run.run_count}")
    origen = er.obtener_publicaciones(er.crear_cliente(), post, subreddit, publicaciones)
    filas = er.extraer_comentarios(origen, max_comentarios)
    print(f"Extraer Reddit: {len(filas)} comentarios")
    return filas


@task(log_prints=True)
def guardar_reddit(filas: list[dict], destino: str) -> int:
    er.guardar_csv(filas, Path(destino))
    print(f"Guardar: {len(filas)} comentarios en {Path(destino).name}")
    return len(filas)


@flow(name="extraccion-reddit", log_prints=True)
def flujo_reddit(post: str | None = None, subreddit: str | None = None, publicaciones: int = 5,
                 max_comentarios: int = 100, salida: str | None = None) -> int:
    filas = extraer_reddit(post, subreddit, publicaciones, max_comentarios)
    destino = salida or str(er.RAIZ / "resultados" / f"reddit_prefect_{datetime.now():%Y%m%d_%H%M%S}.csv")
    return guardar_reddit(filas, destino)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Comentarios de Reddit a CSV con Prefect")
    origen = p.add_mutually_exclusive_group(required=True)
    origen.add_argument("--post")
    origen.add_argument("--subreddit")
    p.add_argument("--publicaciones", type=int, default=5)
    p.add_argument("--max-comentarios", type=int, default=100)
    p.add_argument("--salida")
    a = p.parse_args()
    if faltantes := er.faltan_credenciales():
        print(f"ERROR: faltan variables de entorno: {', '.join(faltantes)}", file=sys.stderr)
        sys.exit(2)
    estado = flujo_reddit(a.post, a.subreddit, a.publicaciones, a.max_comentarios, a.salida,
                          return_state=True)
    print(f"Estado final: {estado.type.value}")
    sys.exit(0 if estado.is_completed() else 1)
