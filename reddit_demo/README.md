# Extracción de comentarios de Reddit (PRAW)

> **Ejemplo OPCIONAL.** El caso práctico principal es [clima_demo/](../clima_demo/README.md) (Open-Meteo, sin credenciales). Se cambió porque el equipo no dispone de credenciales de la API de Reddit.

Extracción **real** desde internet, independiente de las demostraciones de Prefect y Cron (que
simulan el fallo sin conexión). Es de solo lectura y no guarda nombres de autores.

| Archivo | Función |
|---|---|
| `extractor_reddit.py` | Extractor en Python convencional (un intento; código 75 ante fallo de red) |
| `flujo_reddit_prefect.py` | El mismo extractor como flujo de Prefect: `@flow`, `@task` y reintentos automáticos |
| `requirements.txt` | PRAW (opcional, solo para esta carpeta) |

## Flujo con Prefect

```python
@task(retries=3, retry_delay_seconds=2, retry_condition_fn=es_error_de_red)
def extraer_reddit(...): ...      # hasta 4 ejecuciones si falla la red

@task
def guardar_reddit(filas, destino): ...   # solo corre si la extracción tuvo éxito

@flow(name="extraccion-reddit")
def flujo_reddit(...): ...
```

`retry_condition_fn` limita los reintentos a errores de red (`ConnectionError`, `TimeoutError`
y los de `prawcore`). Un error de credenciales o de permisos falla de inmediato, porque
reintentarlo no lo resolvería.

## Configuración

1. Iniciar sesión en Reddit, entrar a <https://www.reddit.com/prefs/apps> y crear una app de
   tipo *script*. Se obtienen un `client_id` y un `client_secret`.
2. Instalar PRAW en el entorno virtual del proyecto:
   ```bash
   pip install -r reddit_demo/requirements.txt
   ```
3. Definir las credenciales como variables de entorno, **nunca** en el código ni en Git:
   ```powershell
   $env:REDDIT_CLIENT_ID = "..."; $env:REDDIT_CLIENT_SECRET = "..."
   $env:REDDIT_USER_AGENT = "script:sesion49-etl:v1.0 (equipo 3)"
   ```
   ```bash
   export REDDIT_CLIENT_ID=... REDDIT_CLIENT_SECRET=...
   export REDDIT_USER_AGENT="script:sesion49-etl:v1.0 (equipo 3)"
   ```

## Probar la extracción real (requiere credenciales)

```bash
python reddit_demo/extractor_reddit.py --subreddit dataengineering --publicaciones 1 --max-comentarios 5
python reddit_demo/flujo_reddit_prefect.py --subreddit dataengineering --publicaciones 1 --max-comentarios 5
```

Resultado esperado: código de salida 0, `Estado final: COMPLETED` en el flujo y un CSV en
`resultados/` con las columnas `comentario_id`, `publicacion_id`, `puntuacion`, `creado_utc` y
`texto`. Para comprobar los reintentos reales, desconecta la red durante la ejecución: el
registro mostrará `Retry 1/3`, `2/3` y `3/3`, y si la red no vuelve, `Estado final: FAILED`.

Antes de subir evidencias de una ejecución real, revisar el CSV: contiene texto escrito por
usuarios de Reddit.

## Pruebas simuladas y pruebas reales

| | Simuladas (en el repositorio) | Reales (cada integrante) |
|---|---|---|
| Archivos | `tests/test_reddit.py`, `tests/test_reddit_prefect.py` | Comandos de la sección anterior |
| Requieren | Python (y Prefect para el flujo) | PRAW, credenciales e internet |
| Comprueban | CSV, reintentos (1, 3 y 4 intentos), sin reintento ante errores no transitorios, código 2 sin credenciales | Autenticación, respuesta real de la API y contenido |
| Estado | 7/7 aprobadas en Windows (`evidencias/10_reddit_simulado_windows.txt`) | **No ejecutadas**: el repositorio no tiene credenciales |

Las pruebas simuladas sustituyen el cliente de Reddit por objetos falsos, así que **no**
demuestran una conexión real.

## Programación diaria a las 12:00 PM

La línea **E** de [`cron_demo/crontab.example`](../cron_demo/crontab.example) ejecuta el flujo de
Prefect a mediodía con rutas absolutas, `flock` y registros separados
(`resultados/reddit_cron.log` y `reddit_cron.error.log`). Las credenciales se cargan desde un
archivo fuera del repositorio:

```bash
mkdir -p ~/.config/sesion49
cat > ~/.config/sesion49/reddit.env <<'FIN'
REDDIT_CLIENT_ID=...
REDDIT_CLIENT_SECRET=...
REDDIT_USER_AGENT=script:sesion49-etl:v1.0 (equipo 3)
PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS=120
FIN
chmod 600 ~/.config/sesion49/reddit.env
```

Para instalarla y comprobarla se siguen los pasos de la sección "Tarea diaria a las 12:00 PM" de
[`cron_demo/README.md`](../cron_demo/README.md), usando la línea E y `tail resultados/reddit_cron.log`.
Se validó la sintaxis con `crontab -n`, y en WSL se ejecutó la línea con entorno mínimo,
credenciales falsas y un intérprete falso. Así se comprobó que carga el archivo de credenciales,
que las rutas son correctas y que los dos registros se escriben. No se hizo una ejecución real.

Referencias: <https://praw.readthedocs.io/> ·
[Reddit Data API Wiki](https://support.reddithelp.com/hc/en-us/articles/16160319875092-Reddit-Data-API-Wiki)
