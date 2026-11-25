"""Pruebas de la versión Prefect, sobre una base de datos temporal."""

import sys
import tempfile
import time
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(RAIZ), str(RAIZ / "tests")]

try:
    import prefect  # noqa: F401
except ImportError:
    raise unittest.SkipTest("Prefect no está instalado")

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId
from prefect.testing.utilities import prefect_test_harness

from demo_prefect import pipeline as pipeline_original
from demo_prefect_escenarios import pipeline
from test_cron import ESPERADO, filas


class Prefect(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        arnes = prefect_test_harness(server_startup_timeout=120)
        arnes.__enter__()
        cls.addClassCleanup(arnes.__exit__, None, None, None)

    def setUp(self):
        t = tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.dir = Path(t.name)

    def leer(self, id_flujo, minimo):
        """Prefect persiste las tareas de forma asíncrona: espera a que estén completas
        y luego 3 s más, para que una tarea inesperada también alcance a registrarse."""
        def consultar():
            return c.read_task_runs(flow_run_filter=FlowRunFilter(
                id=FlowRunFilterId(any_=[id_flujo])))

        with get_client(sync_client=True) as c:
            limite = time.monotonic() + 30
            while time.monotonic() < limite:
                tareas = consultar()
                if len(tareas) >= minimo and all(t.state.is_final() for t in tareas):
                    break
                time.sleep(0.5)
            time.sleep(3)
            return c.read_flow_run(id_flujo), consultar()

    def verificar(self, escenario, estado_final, intentos):
        antes = set(self.dir.glob("*.csv"))
        estado = pipeline(escenario, str(self.dir), return_state=True)
        esperadas = ({"extraer", "transformar", "guardar", "exportar_csv"}
                     if estado_final == "COMPLETED" else {"extraer"})
        flujo, tareas = self.leer(estado.state_details.flow_run_id, len(esperadas))
        por_nombre = {t.name.rsplit("-", 1)[0]: t for t in tareas}
        nuevos = set(self.dir.glob("*.csv")) - antes
        self.assertEqual(flujo.state.type.value, estado_final)
        self.assertEqual(por_nombre["extraer"].run_count, intentos)
        if estado_final == "COMPLETED":
            self.assertEqual(set(por_nombre), esperadas)
            self.assertTrue(all(t.state.is_completed() for t in tareas))
            self.assertEqual(len(nuevos), 1)
            self.assertEqual(filas(nuevos.pop()), ESPERADO)
        else:
            self.assertEqual(set(por_nombre), esperadas)  # dependientes nunca creadas
            self.assertTrue(por_nombre["extraer"].state.is_failed())
            self.assertEqual(nuevos, set())

    def test_normal(self):
        self.verificar("normal", "COMPLETED", 1)

    def test_temporal(self):
        self.verificar("temporal", "COMPLETED", 3)

    def test_permanente(self):
        self.verificar("permanente", "FAILED", 4)

    def test_independencia_y_archivos_previos(self):
        self.verificar("temporal", "COMPLETED", 3)
        self.verificar("temporal", "COMPLETED", 3)
        previos = {p: p.stat().st_mtime_ns for p in self.dir.glob("*.csv")}
        self.verificar("permanente", "FAILED", 4)
        self.assertEqual(previos, {p: p.stat().st_mtime_ns for p in self.dir.glob("*.csv")})

    def test_demo_original_sigue_funcionando(self):
        self.assertEqual(pipeline_original(), 120)


if __name__ == "__main__":
    unittest.main()
