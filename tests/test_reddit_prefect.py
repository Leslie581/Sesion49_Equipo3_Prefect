"""Flujo de Reddit con Prefect usando un cliente SIMULADO (sin PRAW, credenciales ni internet).

Verifica reintentos y dependencias; no demuestra una conexión real con Reddit.
"""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

RAIZ = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(RAIZ / "reddit_demo"), str(RAIZ / "tests")]

try:
    import prefect  # noqa: F401
except ImportError:
    raise unittest.SkipTest("Prefect no está instalado")

from prefect.testing.utilities import prefect_test_harness

import flujo_reddit_prefect as fr
from test_reddit import Comentarios, comentario


class FlujoReddit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        arnes = prefect_test_harness(server_startup_timeout=120)
        arnes.__enter__()
        cls.addClassCleanup(arnes.__exit__, None, None, None)

    def correr(self, fallos, error=ConnectionError):
        """El cliente falla `fallos` veces con `error` y después responde."""
        self.llamadas = 0

        def publicaciones(*_):
            self.llamadas += 1
            if self.llamadas <= fallos:
                raise error("simulado")
            return [SimpleNamespace(id="p1", comments=Comentarios([comentario(1, "a")]))]

        with tempfile.TemporaryDirectory() as d, \
                mock.patch.object(fr.er, "crear_cliente", return_value=None), \
                mock.patch.object(fr.er, "obtener_publicaciones", side_effect=publicaciones):
            destino = Path(d) / "r.csv"
            estado = fr.flujo_reddit(subreddit="x", salida=str(destino), return_state=True)
            return estado, destino.exists()

    def test_exito(self):
        estado, csv = self.correr(0)
        self.assertTrue(estado.is_completed())
        self.assertEqual((self.llamadas, csv), (1, True))

    def test_error_de_red_temporal_se_recupera(self):
        estado, csv = self.correr(2)
        self.assertTrue(estado.is_completed())
        self.assertEqual((self.llamadas, csv), (3, True))

    def test_error_de_red_permanente_cuatro_intentos(self):
        estado, csv = self.correr(99)
        self.assertTrue(estado.is_failed())
        self.assertEqual((self.llamadas, csv), (4, False))

    def test_error_no_transitorio_no_se_reintenta(self):
        estado, csv = self.correr(99, error=PermissionError)
        self.assertTrue(estado.is_failed())
        self.assertEqual((self.llamadas, csv), (1, False))

    def test_sin_credenciales_sale_con_2(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith("REDDIT_")}
        r = subprocess.run([sys.executable, str(RAIZ / "reddit_demo" / "flujo_reddit_prefect.py"),
                            "--subreddit", "x"], env=env, capture_output=True, text=True,
                           encoding="utf-8")
        self.assertEqual(r.returncode, 2)


if __name__ == "__main__":
    unittest.main()
