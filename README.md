# Sesion49_Equipo3_Prefect

Orquestación de flujos de datos con **Prefect** — Equipo 3, Ingeniería de Datos (7mo semestre, Unidad III).

## Descripción
El proyecto compara dos formas de automatizar un mismo ETL: **Prefect** y **Cron**. El proceso extrae `[10, 20, 30]`, transforma a `[20, 40, 60]` y guarda un CSV con total 120. No usa internet ni APIs externas: los fallos se simulan.

`demo_prefect.py` implementa el flujo original (extraer → transformar → guardar). La extracción simula una desconexión temporal en los dos primeros intentos y se recupera gracias a `@task(retries=3, retry_delay_seconds=2)`. `demo_prefect_escenarios.py` reutiliza sus tareas y agrega tres escenarios y la salida CSV:

| Escenario | Comportamiento |
|---|---|
| `normal` | Éxito al primer intento |
| `temporal` | Dos fallos y recuperación en el tercer intento |
| `permanente` | Cuatro ejecuciones fallidas; el flujo se detiene y no genera CSV |

## Contenido
```text
├── demo_prefect.py               demostración original
├── demo_prefect_escenarios.py    tres escenarios y CSV
├── programacion_recurrente.py    ejecución cada 60 s
├── requirements.txt
├── cron_demo/                    versión con Cron y Bash
├── reddit_demo/                  extractor opcional de comentarios (PRAW)
├── docs/COMPARATIVA_CRON_PREFECT.md
├── tests/                        pruebas y generador de evidencias
├── evidencias/                   salidas reales
├── reporte/                      reporte individual y su fuente
└── resultados/                   CSV y registros (no versionados)
```
- `Equipo3_Orquestacion_Prefect.pptx` / `.pdf`: presentación.
- `Equipo_3_Actividades_Prefect.docx`: actividades.
- `Guion_Exposicion.md`: guion de la exposición.

## Requisitos
- Python 3.11 o superior, menor que 3.16 (lo exige Prefect 3.9.5, la versión probada)
- Prefect 3 (`requirements.txt`)
- Para Cron: Linux o WSL con Bash, `cron` y `flock`

## Instalación
Windows (PowerShell):
```powershell
python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Linux o WSL:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Ejecución
```powershell
python demo_prefect.py
python demo_prefect_escenarios.py normal
python demo_prefect_escenarios.py temporal
python demo_prefect_escenarios.py permanente   # termina con código 1
```

Si aparece `Timed out while attempting to connect to ephemeral Prefect API server`, se amplía la espera del servidor temporal:
```powershell
$env:PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS = "120"        # PowerShell
```
```bash
export PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS=120          # Linux
```

## Prefect UI
Terminal 1, con el entorno virtual activo:
```powershell
prefect server start
```
Terminal 2:
```powershell
$env:PREFECT_API_URL = "http://127.0.0.1:4200/api"      # Linux: export PREFECT_API_URL=...
python demo_prefect_escenarios.py temporal
```
Abrir <http://127.0.0.1:4200> para ver flujos, tareas, estados y registros.

## Programación recurrente
Con el servidor activo y `PREFECT_API_URL` configurada:
```powershell
python programacion_recurrente.py temporal
```
Ejecuta el flujo cada 60 segundos mientras el proceso siga abierto. `Ctrl+C` lo detiene y pausa la programación.

## Ejecución mediante Cron
En Linux o WSL (Cron no funciona de forma nativa en PowerShell):
```bash
python3 cron_demo/demo_cron.py etl --escenario temporal   # un intento, sin recuperación
bash cron_demo/reintentos.sh temporal                     # reintentos manuales
```
La programación diaria (medianoche o 12:00 PM con `0 12 * * *`), su comprobación y `flock` se explican en [cron_demo/README.md](cron_demo/README.md).

## Extractor opcional de Reddit
`reddit_demo/extractor_reddit.py` extrae comentarios reales con PRAW (solo lectura, credenciales por variables de entorno). No forma parte de las demostraciones anteriores ni de `requirements.txt`; ver [reddit_demo/README.md](reddit_demo/README.md).

## Comparativa
| | Cron sin reintentos | Cron con reintentos | Prefect |
|---|---|---|---|
| Fallo temporal | Termina con error | Se recupera | Se recupera |
| Código para reintentar | No tiene | Script Bash | Dos parámetros de `@task` |
| Monitoreo | Correo o archivos | Archivos | Estados e interfaz web |
| Infraestructura | Cron | Cron y Bash | Prefect, servidor y proceso activo |

Análisis completo: [docs/COMPARATIVA_CRON_PREFECT.md](docs/COMPARATIVA_CRON_PREFECT.md).

## Pruebas y resultados
```bash
python -m unittest discover -s tests -v
```

| Escenario | Cron sin reintentos | Cron con reintentos | Prefect |
|---|---|---|---|
| normal | 1 intento, código 0 | 1 intento, código 0 | 1 intento, Completed |
| temporal | 1 intento, código 75 | 3 intentos, código 0 | 3 intentos, Completed |
| permanente | 1 intento, código 75 | 4 intentos, código 1 | 4 intentos, Failed, código 1 |

Linux (WSL): 15 de 15 aprobadas. Windows: 8 aprobadas y 7 omitidas porque requieren Bash de Linux. Detalle y observaciones en [evidencias/](evidencias/README.md).

## Referencias
- Prefect: <https://docs.prefect.io/>
- `crontab(5)`: <https://man7.org/linux/man-pages/man5/crontab.5.html>
- `flock(1)`: <https://man7.org/linux/man-pages/man1/flock.1.html>
