# Evidencias

Salidas reales del 25 de noviembre de 2026. Solo se sustituyeron rutas y usuario por
`<proyecto>`, `<venv>`, `<ruta>` y `<usuario>`.

| Archivo | Contenido |
|---|---|
| `00_entorno.txt` | Versiones utilizadas en Linux (WSL 2) |
| `01_cron_sin_reintentos.txt` | Tres escenarios, un solo intento |
| `02_cron_con_reintentos.txt` | Tres escenarios con `reintentos.sh` |
| `03_crontab_y_flock.txt` | Línea de crontab en entorno mínimo y prueba de solapamiento |
| `04_prefect_demo_original.txt` | `demo_prefect.py` sin cambios |
| `05_prefect_escenarios.txt` | Tres escenarios con Prefect |
| `06_csv_generados.txt` | Contenido de los CSV |
| `07_prefect_recurrente.txt` | Programación cada 60 s durante 150 s |
| `08_pruebas_linux.txt` | 15 pruebas: 15 aprobadas |
| `09_pruebas_windows.txt` | 15 pruebas: 8 aprobadas y 7 omitidas (requieren Bash de Linux) |
| `10_reddit_simulado_windows.txt` | Reddit con cliente **simulado**: 7/7 aprobadas (no es una extracción real) |
| `11_windows_sin_extensiones.txt` | Prefect con `whenever` y SQLAlchemy sin `.pyd` |
| `12_clima_cron_escenarios.txt` | **Open-Meteo**: tres escenarios con `cron_clima.sh` (consultas reales) |
| `13_clima_prefect_escenarios.txt` | **Open-Meteo**: tres escenarios con Prefect (consultas reales) |
| `14_clima_csv_historico.txt` | Histórico CSV y verificación: filas, unidades y 0 duplicados |
| `15_clima_pruebas_linux.txt` | `tests/test_clima.py` en WSL, incluida una consulta real |
| `16_crontab_wsl_real.txt` | Ejecución **efectiva** por el servicio cron de WSL (minuto cercano) y línea definitiva de 12:00 PM |
| `18_prefect_ui_escenarios.txt` | Tres escenarios contra el servidor Prefect local; son las ejecuciones de las capturas del reporte |
| `17_prefect_serve_ui.txt` | Servidor Prefect + `serve` con minuto cercano; programación definitiva `0 12 * * *` en `America/Mexico_City` |

Los 01 a 08 se regeneran con `PY=<venv>/bin/python bash tests/generar_evidencias.sh`, y los 12 a 15 con
`PY=<venv>/bin/python bash tests/generar_evidencias_clima.sh`. Los 16 y 17 requieren instalar una
línea en crontab y esperar al minuto programado; el procedimiento está en `clima_demo/README.md`.

## Observaciones

- No hay evidencias de extracción real de Reddit: requiere credenciales personales.
- En 03 (demostración base) no se instaló ninguna tarea en crontab ni se esperó una medianoche real. Se validó la sintaxis
  con `crontab -n` y se ejecutó la línea C con un entorno mínimo similar al de Cron.
- En una ejecución previa en WSL, `demo_prefect.py` falló una vez porque el servidor temporal de
  Prefect no inició en los 20 s predeterminados. Las evidencias usan
  `PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS=120`.
- En esa misma ejecución fallaron 3 pruebas de Prefect: consultaban las tareas antes de que se
  guardaran, pues Prefect las registra de forma asíncrona. Se corrigió la prueba y se repitió todo.
- En `05`, el reloj de WSL retrocede (intentos 1 y 2 de `temporal`). Es un ajuste de hora de la
  máquina virtual; el orden de las líneas es el real. Las pruebas de Cron miden la espera con un
  reloj monótono.
- En Windows, al cerrar, Prefect muestra un `PermissionError` al borrar su base temporal de
  pruebas. Ocurre después del resultado `OK`.
- En `05`, Prefect registra errores `CancelledError` de su base SQLite y avisos de eventos pendientes
  al cerrar el servidor temporal, después del estado final. No cambian el resultado ni el código de
  salida; con un servidor propio (`prefect server start`) no aparecen en `07`.
- Las trazas de `ConnectionError` en `permanente` son el comportamiento esperado.
- Caso Open-Meteo: los datos de 12 a 17 son respuestas reales de la API. Solo las desconexiones de
  los escenarios `temporal` y `permanente` son simuladas. En 16 el servicio cron sí ejecutó la tarea:
  se usó un minuto cercano y luego se dejó instalada la línea definitiva `0 12 * * *`. No se esperó a
  un mediodía real. La latitud y la longitud del CSV son las de la celda del modelo que devuelve la
  API, no las solicitadas.
- En 17 se usan `prefect deployment` y `prefect flow-run`, que consultan la misma API que la UI. Las
  capturas reales de la interfaz (ejecuciones de 18) están en `reporte/figuras/ui_*.png` y en el reporte.
