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

Los 01 a 08 se regeneran con `PY=<venv>/bin/python bash tests/generar_evidencias.sh`.

## Observaciones

- No se instaló ninguna tarea en crontab ni se esperó una medianoche real. Se validó la sintaxis
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
