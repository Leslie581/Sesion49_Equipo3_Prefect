"""Pruebas del extractor de Reddit con objetos simulados (sin PRAW ni internet)."""

import csv
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "reddit_demo"))
from extractor_reddit import CAMPOS, extraer_comentarios, guardar_csv  # noqa: E402


class Comentarios(list):
    def replace_more(self, limit):
        self.limite = limit

    def list(self):
        return self


def comentario(i, texto):
    return SimpleNamespace(id=f"c{i}", score=i, created_utc=0, body=texto, author="no-se-guarda")


class Reddit(unittest.TestCase):
    def test_aplanado_y_csv(self):
        pub = SimpleNamespace(id="p1", comments=Comentarios(
            [comentario(1, "hola\nmundo"), comentario(2, "b"), comentario(3, "c")]))
        filas = extraer_comentarios([pub], max_comentarios=2)
        self.assertEqual(pub.comments.limite, 0)
        self.assertEqual([f["comentario_id"] for f in filas], ["c1", "c2"])
        self.assertEqual(filas[0]["texto"], "hola mundo")
        self.assertEqual(filas[0]["creado_utc"], "1970-01-01T00:00:00+00:00")
        with tempfile.TemporaryDirectory() as d:
            destino = Path(d) / "r.csv"
            guardar_csv(filas, destino)
            with destino.open(encoding="utf-8", newline="") as f:
                leidas = list(csv.reader(f))
        self.assertEqual(leidas[0], CAMPOS)
        self.assertNotIn("no-se-guarda", str(leidas))

    def test_sin_credenciales_sale_con_2(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith("REDDIT_")}
        r = subprocess.run([sys.executable, str(RAIZ / "reddit_demo" / "extractor_reddit.py"),
                            "--subreddit", "x"], env=env, capture_output=True, text=True,
                           encoding="utf-8")
        self.assertEqual(r.returncode, 2)
        self.assertIn("REDDIT_CLIENT_ID", r.stderr)


if __name__ == "__main__":
    unittest.main()
