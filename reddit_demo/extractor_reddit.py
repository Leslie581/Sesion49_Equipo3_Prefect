#!/usr/bin/env python3
"""Extractor OPCIONAL de comentarios de Reddit con PRAW (solo lectura).

Es independiente de las demostraciones de Prefect y Cron, que no usan internet.
Las credenciales se leen de variables de entorno; nunca se escriben en el código:
  REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT

Uso:
  python reddit_demo/extractor_reddit.py --post https://www.reddit.com/r/.../comments/xxxx/...
  python reddit_demo/extractor_reddit.py --subreddit dataengineering --publicaciones 5

Códigos de salida: 0 éxito, 1 error, 2 configuración inválida, 75 fallo de red (reintentable).
"""

import argparse
import csv
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
VARIABLES = ("REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_USER_AGENT")
CAMPOS = ["comentario_id", "publicacion_id", "puntuacion", "creado_utc", "texto"]
EX_TEMPFAIL = 75


def extraer_comentarios(publicaciones, max_comentarios: int) -> list[dict]:
    """Aplana los comentarios de las publicaciones. No guarda autores (datos personales)."""
    filas = []
    for pub in publicaciones:
        pub.comments.replace_more(limit=0)  # omite "cargar más": evita cientos de peticiones
        for c in pub.comments.list()[:max_comentarios]:
            filas.append({
                "comentario_id": c.id,
                "publicacion_id": pub.id,
                "puntuacion": c.score,
                "creado_utc": datetime.fromtimestamp(c.created_utc, timezone.utc).isoformat(),
                "texto": " ".join(c.body.split()),
            })
    return filas


def faltan_credenciales() -> list[str]:
    return [v for v in VARIABLES if not os.environ.get(v)]


def crear_cliente():
    """Cliente PRAW de solo lectura con las credenciales del entorno."""
    import praw

    reddit = praw.Reddit(client_id=os.environ["REDDIT_CLIENT_ID"],
                         client_secret=os.environ["REDDIT_CLIENT_SECRET"],
                         user_agent=os.environ["REDDIT_USER_AGENT"])
    reddit.read_only = True
    return reddit


def obtener_publicaciones(reddit, post: str | None, subreddit: str | None, cantidad: int):
    if post:
        return [reddit.submission(url=post)]
    return reddit.subreddit(subreddit).hot(limit=cantidad)


def errores_de_red() -> tuple:
    """Errores transitorios que vale la pena reintentar."""
    try:
        import prawcore
    except ImportError:
        return (ConnectionError, TimeoutError)
    return (ConnectionError, TimeoutError,
            prawcore.exceptions.RequestException, prawcore.exceptions.ServerError)


def guardar_csv(filas: list[dict], destino: Path) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporal = destino.with_suffix(".tmp")
    with temporal.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CAMPOS)
        w.writeheader()
        w.writerows(filas)
    os.replace(temporal, destino)


def main() -> int:
    p = argparse.ArgumentParser(description="Extrae comentarios de Reddit a CSV (PRAW)")
    origen = p.add_mutually_exclusive_group(required=True)
    origen.add_argument("--post", help="URL de una publicación")
    origen.add_argument("--subreddit", help="nombre del subreddit, sin r/")
    p.add_argument("--publicaciones", type=int, default=5, help="publicaciones 'hot' a leer")
    p.add_argument("--max-comentarios", type=int, default=100, help="por publicación")
    p.add_argument("--salida", type=Path)
    a = p.parse_args()

    faltantes = faltan_credenciales()
    if faltantes:
        print(f"ERROR: faltan variables de entorno: {', '.join(faltantes)}", file=sys.stderr)
        return 2
    try:
        import prawcore
    except ImportError:
        print("ERROR: instale PRAW con: pip install -r reddit_demo/requirements.txt", file=sys.stderr)
        return 2

    try:
        publicaciones = obtener_publicaciones(crear_cliente(), a.post, a.subreddit, a.publicaciones)
        filas = extraer_comentarios(publicaciones, a.max_comentarios)
    except errores_de_red() as e:
        print(f"ERROR de red (reintentable): {e}", file=sys.stderr)
        return EX_TEMPFAIL
    except prawcore.exceptions.PrawcoreException as e:  # credenciales, permisos, no encontrado
        print(f"ERROR de Reddit: {e}", file=sys.stderr)
        return 1

    destino = a.salida or RAIZ / "resultados" / f"reddit_{datetime.now():%Y%m%d_%H%M%S}.csv"
    guardar_csv(filas, destino)
    print(f"{len(filas)} comentarios guardados en {destino.name}")
    return 0


if __name__ == "__main__":
    for flujo in (sys.stdout, sys.stderr):
        flujo.reconfigure(encoding="utf-8")
    sys.exit(main())
