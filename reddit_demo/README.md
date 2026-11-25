# Extractor opcional de comentarios de Reddit (PRAW)

Ejemplo de una extracción **real** desde internet, independiente de las demostraciones de
Prefect y Cron, que siguen simulando el fallo sin conexión. Es de solo lectura y no guarda
nombres de autores.

## Configuración

1. Iniciar sesión en Reddit, entrar a <https://www.reddit.com/prefs/apps> y crear una app de
   tipo *script*. Se obtienen un `client_id` y un `client_secret`.
2. Instalar PRAW, solo si se usará este extractor:
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

## Ejecución

```bash
python reddit_demo/extractor_reddit.py --subreddit dataengineering --publicaciones 3
python reddit_demo/extractor_reddit.py --post "https://www.reddit.com/r/<sub>/comments/<id>/..."
```

El CSV queda en `resultados/` con las columnas `comentario_id`, `publicacion_id`, `puntuacion`,
`creado_utc` y `texto`. Ante un fallo de red el programa sale con código **75**, así que puede
envolverse con la misma lógica de reintentos de `cron_demo/reintentos.sh` o con
`@task(retries=3, retry_delay_seconds=2)` en Prefect.

## Pruebas

`tests/test_reddit.py` verifica la transformación y el CSV con objetos simulados; no necesita
PRAW, credenciales ni internet. La conexión real con Reddit **no** se probó en este repositorio
porque requiere credenciales personales.

Referencias: <https://praw.readthedocs.io/> ·
[Reddit Data API Wiki](https://support.reddithelp.com/hc/en-us/articles/16160319875092-Reddit-Data-API-Wiki)
