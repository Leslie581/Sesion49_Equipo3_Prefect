"""Pruebas del ETL de clima (Open-Meteo). Las de transformar/guardar no usan red;
RealAPI consulta la API real y se omite si no hay conexión."""

import csv
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "clima_demo"))
import etl_clima as etl  # noqa: E402


def respuesta(temp=25.0, hum=60, prec=0.0, viento=5.0, hora="2026-11-25T12:00"):
    unidades = {"temperature_2m": "°C", "relative_humidity_2m": "%", "precipitation": "mm",
                "wind_speed_10m": "km/h"}
    return [{"latitude": la, "longitude": lo, "timezone": etl.ZONA_HORARIA, "current_units": unidades,
             "current": {"time": hora, "temperature_2m": temp, "relative_humidity_2m": hum,
                         "precipitation": prec, "wind_speed_10m": viento}}
            for la, lo in etl.CIUDADES.values()]


class Transformar(unittest.TestCase):
    def test_conserva_ciudad_unidades_fuente(self):
        filas, descartes = etl.transformar(respuesta())
        self.assertEqual(descartes, [])
        self.assertEqual([f["ciudad"] for f in filas], list(etl.CIUDADES))
        self.assertEqual(filas[0]["temperatura_unidad"], "°C")
        self.assertEqual(filas[0]["viento_unidad"], "km/h")
        self.assertIn("Open-Meteo", filas[0]["fuente"])

    def test_descarta_invalidos(self):
        crudo = respuesta()
        crudo[0]["current"]["relative_humidity_2m"] = 140
        crudo[1]["current"]["temperature_2m"] = None
        filas, descartes = etl.transformar(crudo)
        self.assertEqual([f["ciudad"] for f in filas], ["Manzanillo"])
        self.assertEqual(len(descartes), 2)

    def test_todo_invalido_es_error_permanente(self):
        with self.assertRaises(etl.ErrorPermanente):
            etl.transformar(respuesta(prec=-1))


class Guardar(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.csv = self.dir / "h.csv"

    def tearDown(self):
        shutil.rmtree(self.dir)

    def test_historico_sin_duplicados(self):
        self.assertEqual(etl.guardar(etl.transformar(respuesta())[0], self.csv), (3, 3))
        self.assertEqual(etl.guardar(etl.transformar(respuesta())[0], self.csv), (0, 3))
        self.assertEqual(etl.guardar(etl.transformar(respuesta(hora="2026-11-26T12:00"))[0], self.csv), (3, 6))
        with self.csv.open(encoding="utf-8") as f:
            self.assertEqual(csv.DictReader(f).fieldnames, etl.COLUMNAS)


class Escenarios(unittest.TestCase):
    def test_simulacion(self):
        etl.simular_fallo("normal", 1)
        for i in (1, 2):
            self.assertRaises(ConnectionError, etl.simular_fallo, "temporal", i)
        etl.simular_fallo("temporal", 3)
        for i in range(1, 5):
            self.assertRaises(ConnectionError, etl.simular_fallo, "permanente", i)

    def test_errores_http(self):
        import urllib.error
        for codigo, esperado in ((503, etl.ErrorTransitorio), (429, etl.ErrorTransitorio),
                                 (400, etl.ErrorPermanente)):
            err = urllib.error.HTTPError("u", codigo, "x", {}, None)
            err.read = lambda: b""
            with mock.patch("urllib.request.urlopen", side_effect=err):
                self.assertRaises(esperado, etl.extraer)


@unittest.skipUnless(shutil.which("bash") and sys.platform != "win32", "requiere Bash de Linux")
class CronReintentos(unittest.TestCase):
    def correr(self, escenario):
        d = Path(tempfile.mkdtemp()); self.addCleanup(shutil.rmtree, d)
        env = {"PATH": "/usr/bin:/bin", "PYTHON_BIN": sys.executable, "ESPERA": "0",
               "CSV_CLIMA": str(d / "h.csv"), "LOG_CLIMA": str(d / "c.log")}
        r = subprocess.run(["bash", str(RAIZ / "clima_demo" / "cron_clima.sh"), escenario],
                           capture_output=True, text=True, env=env, timeout=120)
        return r, d / "h.csv"

    def test_permanente_agota_4_intentos_sin_csv(self):
        r, h = self.correr("permanente")
        self.assertEqual(r.returncode, 1)
        self.assertIn("Intento 4/4", r.stdout)
        self.assertFalse(h.exists())


class RealAPI(unittest.TestCase):
    def test_consulta_real(self):
        try:
            crudo = etl.extraer()
        except etl.ErrorTransitorio as e:
            self.skipTest(f"sin conexión: {e}")
        filas, _ = etl.transformar(crudo)
        self.assertEqual(len(filas), 3)
        self.assertEqual(filas[0]["zona_horaria"], etl.ZONA_HORARIA)


if __name__ == "__main__":
    unittest.main()
