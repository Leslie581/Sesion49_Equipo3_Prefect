# Sesion49_Equipo3_Prefect

Orquestación de flujos de datos con **Prefect** — Equipo 3, Ingeniería de Datos (7mo semestre, Unidad III).

## Caso práctico principal: clima de Colima (Open-Meteo)
Como el equipo no dispone de las credenciales OAuth que exige la API de Reddit, la fuente principal de la práctica es **[Open-Meteo](https://open-meteo.com/en/docs)**. Es una API meteorológica pública que no requiere credenciales para uso no comercial. El ETL extrae datos **reales** de temperatura, humedad, precipitación y viento de **Colima, Villa de Álvarez y Manzanillo**. Valida, limpia y acumula un histórico CSV sin duplicados que conserva fecha, hora, zona horaria, ciudad, unidades y fuente. El mismo código lo ejecutan **Cron** (reintentos manuales) y **Prefect** (reintentos automáticos), todos los días a las **12:00 PM, hora de `America/Mexico_City`**.

```bash
python3 clima_demo/etl_clima.py                                  # una consulta real
bash clima_demo/cron_clima.sh temporal                           # Cron con reintentos (Linux/WSL)
python clima_demo/flujo_clima_prefect.py --escenario temporal   # Prefect con reintentos
python clima_demo/flujo_clima_prefect.py --servir               # Prefect: diario 12:00 PM
```

| Escenario | Cron (`cron_clima.sh`) | Prefect |
|---|---|---|
| `normal` | 1 intento, código 0, datos reales | 1 intento, Completed |
| `temporal` (2 desconexiones simuladas y luego una consulta real) | 3 intentos, código 0 | 3 intentos, Completed |
| `permanente` (desconexión simulada en todos los intentos) | 4 intentos, código 1, sin datos | 4 intentos, Failed, código 1 |

Reporte técnico completo con capturas de Prefect UI: [reporte/Sesion_49_Equipo_3.pdf](reporte/Sesion_49_Equipo_3.pdf) ([DOCX](reporte/Sesion_49_Equipo_3.docx)).

Diseño, justificación con documentación oficial, zona horaria, programación y monitoreo: [clima_demo/README.md](clima_demo/README.md). Evidencias reales: archivos 12 a 17 de [evidencias/](evidencias/README.md), incluida una ejecución efectiva de `crontab` en WSL.

## Demostración base (simulada)

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
├── clima_demo/                   CASO PRINCIPAL: ETL Open-Meteo (Cron y Prefect)
├── cron_demo/                    versión con Cron y Bash
├── reddit_demo/                  ejemplo OPCIONAL de Reddit (PRAW, requiere credenciales)
├── docs/COMPARATIVA_CRON_PREFECT.md
├── tests/                        pruebas y generador de evidencias
├── evidencias/                   salidas reales
├── reporte/                      reporte técnico (PDF, DOCX), fuente y figuras
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

## Ejemplo opcional: extracción de Reddit (PRAW + Prefect)
No es necesario para la práctica: el caso principal es `clima_demo/`. Se conserva como referencia.

`reddit_demo/extractor_reddit.py` extrae comentarios reales con PRAW (solo lectura, credenciales en variables de entorno). `reddit_demo/flujo_reddit_prefect.py` lo integra con Prefect mediante `@flow`, `@task` y `retries=3, retry_delay_seconds=2`, y reintenta solo los errores de red. La línea E de `cron_demo/crontab.example` lo programa diariamente a las 12:00 PM.

```bash
pip install -r reddit_demo/requirements.txt   # opcional; también requiere credenciales
python reddit_demo/flujo_reddit_prefect.py --subreddit dataengineering --publicaciones 1 --max-comentarios 5
```
Las pruebas del repositorio usan un cliente **simulado** (7/7 aprobadas). La extracción real **no** se ejecutó porque requiere credenciales. Pasos y diferencias en [reddit_demo/README.md](reddit_demo/README.md).

## Solución de problemas en Windows: `whenever` y SQLAlchemy
Prefect depende de `whenever` y de SQLAlchemy, que incluyen extensiones compiladas (`.pyd`). Si **Control inteligente de aplicaciones** o una directiva de control de aplicaciones de Windows las bloquea, al importar Prefect aparece `ImportError: DLL load failed while importing _whenever` (o `_row_cy`, `_processors_cy`, etc.), junto con un mensaje que indica que una directiva de Control de aplicaciones bloqueó el archivo. Ninguno de los dos paquetes cambia solo a Python puro en ese caso: las versiones instaladas (`whenever` 0.11.0 y SQLAlchemy 2.1.9) dejan pasar el error.

Solución: reinstalar ambos paquetes **sin extensiones compiladas**, dentro del entorno virtual:
```powershell
$env:WHENEVER_NO_BUILD_RUST_EXT = "1"
pip install --force-reinstall --no-deps --no-binary whenever whenever
$env:DISABLE_SQLALCHEMY_CEXT = "1"
pip install --force-reinstall --no-deps --no-binary sqlalchemy sqlalchemy
```
Se comprueba con:
```powershell
python -c "import whenever; print(whenever._EXTENSION_LOADED)"   # False = Python puro
python -c "from sqlalchemy.util._has_cython import HAS_CYEXTENSION; print(HAS_CYEXTENSION)"   # False
```
**Qué se verificó:** con copias de ambos paquetes sin archivos `.pyd`, el resultado es el mismo que tras esa reinstalación. Con ellas, Prefect 3.9.5 ejecutó el escenario `temporal` (3 intentos, `COMPLETED`); ver `evidencias/11_windows_sin_extensiones.txt`. En el equipo de prueba **no** hubo bloqueo, así que no se reprodujo el error original ni se ejecutaron los comandos `pip` anteriores. Las variables y opciones provienen de la documentación oficial ([whenever, FAQ](https://whenever.readthedocs.io/en/latest/faq.html); [SQLAlchemy, instalación](https://docs.sqlalchemy.org/en/21/intro.html)). La versión pura es algo más lenta, pero esa diferencia no afecta a estas demostraciones. Alternativas: excluir la carpeta del entorno virtual de esa directiva (si la administración del equipo lo permite) o usar WSL.

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

Linux (WSL): 15 de 15 aprobadas. Windows: 8 aprobadas y 7 omitidas porque requieren Bash de Linux.
Caso Open-Meteo (`tests/test_clima.py`, incluye una consulta real): 8 de 8 en WSL; 7 aprobadas y 1 omitida (requiere Bash) en Windows. Evidencias 12 a 15 con `PY=<venv>/bin/python bash tests/generar_evidencias_clima.sh`. Detalle y observaciones en [evidencias/](evidencias/README.md).

## Referencias
- Open-Meteo: <https://open-meteo.com/en/docs> y [términos](https://open-meteo.com/en/terms) (CC BY 4.0)
- Prefect: <https://docs.prefect.io/>
- `crontab(5)`: <https://man7.org/linux/man-pages/man5/crontab.5.html>
- `flock(1)`: <https://man7.org/linux/man-pages/man1/flock.1.html>
