"""Pruebas de la versión Cron. Uso: python -m unittest discover -s tests -v"""

import csv
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
DEMO = RAIZ / "cron_demo" / "demo_cron.py"
SCRIPT = RAIZ / "cron_demo" / "reintentos.sh"


def filas(ruta):
    with open(ruta, encoding="utf-8", newline="") as f:
        return list(csv.reader(f))


ESPERADO = [["concepto", "valor"], ["registro_1", "20"], ["registro_2", "40"],
            ["registro_3", "60"], ["total", "120"]]


class Base(unittest.TestCase):
    def setUp(self):
        t = tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.dir = Path(t.name)


class SinReintentos(Base):
    def correr(self, escenario):
        self.csv = self.dir / "r.csv"
        return subprocess.run(
            [sys.executable, str(DEMO), "etl", "--escenario", escenario, "--salida", str(self.csv)],
            capture_output=True, text=True, encoding="utf-8")

    def test_normal(self):
        r = self.correr("normal")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(filas(self.csv), ESPERADO)

    def test_un_fallo_no_se_reintenta(self):
        for e in ("temporal", "permanente"):
            with self.subTest(e):
                r = self.correr(e)
                self.assertEqual(r.returncode, 75)
                self.assertEqual(r.stdout.count("Extraer: intento"), 1)
                self.assertNotIn("Transformar", r.stdout)
                self.assertEqual(list(self.dir.iterdir()), [])

    def test_escenario_invalido(self):
        self.assertEqual(self.correr("otro").returncode, 2)


@unittest.skipIf(os.name == "nt", "requiere Bash en Linux o WSL")
class ConReintentos(Base):
    def setUp(self):
        super().setUp()
        self.out, self.tmp = self.dir / "res", self.dir / "tmp"
        self.out.mkdir()
        self.tmp.mkdir()
        self.env = {**os.environ, "PYTHON_BIN": sys.executable, "TMPDIR": str(self.tmp),
                    "DIR_RESULTADOS": str(self.out)}

    def correr(self, escenario):
        return subprocess.run(["bash", str(SCRIPT), escenario], env=self.env,
                              capture_output=True, text=True, encoding="utf-8")

    def verificar(self, escenario, codigo, intentos):
        antes = set(self.out.glob("*.csv"))
        t0 = time.monotonic()
        r = self.correr(escenario)
        dt = time.monotonic() - t0
        nuevos = set(self.out.glob("*.csv")) - antes
        self.assertEqual(r.returncode, codigo, r.stdout + r.stderr)
        self.assertEqual(r.stdout.count("Extraer: intento"), intentos)
        self.assertGreaterEqual(dt, 2 * (intentos - 1))  # espera real de 2 s
        self.assertEqual(list(self.tmp.iterdir()), [], "estado temporal sin limpiar")
        if codigo == 0:
            self.assertEqual(len(nuevos), 1)
            self.assertEqual(filas(nuevos.pop()), ESPERADO)
        else:
            self.assertEqual(nuevos, set())
            self.assertNotIn("Transformar:", r.stdout)
            self.assertNotIn("Guardar:", r.stdout)
        return r

    def test_normal(self):
        self.verificar("normal", 0, 1)

    def test_temporal(self):
        r = self.verificar("temporal", 0, 3)
        pids = {x.split("pid ")[1] for x in r.stdout.splitlines() if "pid " in x}
        self.assertEqual(len(pids), 3)  # un proceso Python por intento

    def test_permanente(self):
        self.verificar("permanente", 1, 4)
        self.assertIn("Reintentos agotados", next(self.out.glob("*.log")).read_text("utf-8"))

    def test_independencia_y_archivos_previos(self):
        self.verificar("temporal", 0, 3)
        self.verificar("temporal", 0, 3)
        previos = {p: p.stat().st_mtime_ns for p in self.out.glob("*.csv")}
        self.verificar("permanente", 1, 4)
        self.assertEqual(previos, {p: p.stat().st_mtime_ns for p in self.out.glob("*.csv")})

    def test_uso_incorrecto(self):
        self.assertEqual(self.correr("otro").returncode, 2)

    def test_limpia_al_recibir_sigterm(self):
        p = subprocess.Popen(["bash", str(SCRIPT), "permanente"], env=self.env,
                             stdout=subprocess.DEVNULL)
        time.sleep(1)
        self.assertEqual(len(list(self.tmp.iterdir())), 1)
        p.terminate()
        self.assertEqual(p.wait(10), 143)
        self.assertEqual(list(self.tmp.iterdir()), [])

    @unittest.skipUnless(shutil.which("flock"), "requiere flock")
    def test_flock_impide_solapamiento(self):
        cmd = ["flock", "-n", "-E", "99", str(self.dir / "l"), "bash", str(SCRIPT), "temporal"]
        a = subprocess.Popen(cmd, env=self.env, stdout=subprocess.DEVNULL)
        time.sleep(0.5)
        b = subprocess.run(cmd, env=self.env, stdout=subprocess.DEVNULL)
        self.assertEqual(b.returncode, 99)
        self.assertEqual(a.wait(30), 0)
        self.assertEqual(len(list(self.out.glob("*.csv"))), 1)


if __name__ == "__main__":
    unittest.main()
