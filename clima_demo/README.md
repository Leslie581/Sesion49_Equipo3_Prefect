# Caso práctico principal: clima de Colima con Open-Meteo

ETL real que se ejecuta todos los días a las 12:00 PM (hora del centro de México) con **Cron** y con
**Prefect**, usando el mismo código. Consulta las condiciones actuales de **Colima, Villa de Álvarez
y Manzanillo**: temperatura, humedad relativa, precipitación y velocidad del viento.

## ¿Por qué Open-Meteo en lugar de Reddit?

| Criterio | Reddit (PRAW) | Open-Meteo |
|---|---|---|
| Credenciales | OAuth: `client_id`, `client_secret` y aplicación registrada | Ninguna para uso no comercial |
| Alta de acceso | Desde noviembre de 2025 el acceso nuevo requiere solicitud y aprobación (Responsible Builder Policy) | Inmediato |
| Reproducible por cualquier integrante | No sin credenciales personales | Sí |
| Datos | Comentarios (texto libre) | Series numéricas con unidades y zona horaria |

Fuentes oficiales:
- Open-Meteo, [documentación de la API](https://open-meteo.com/en/docs). El parámetro `apikey` se
  describe como *"Only required to commercial use to access reserved API resources for customers"*.
  También indica que *"Multiple coordinates can be comma separated"* y que *"Current conditions are
  based on 15-minutely weather model data"*.
- Open-Meteo, [términos de uso](https://open-meteo.com/en/terms): uso no comercial gratuito, con
  *"Less than 10'000 API calls per day, 5'000 per hour and 600 per minute"* y licencia **CC BY 4.0**.
  Este ETL hace **una** llamada por ejecución, porque pide las tres ciudades juntas.
- Reddit: [Responsible Builder Policy](https://support.reddithelp.com/hc/en-us/articles/42728983564564-Responsible-Builder-Policy),
  [Data API Wiki](https://support.reddithelp.com/hc/en-us/articles/16160319875092-Reddit-Data-API-Wiki) y
  [anuncio en r/redditdev (11 nov 2025)](https://www.reddit.com/r/redditdev/comments/1oug31u/).
  Las páginas de ayuda de Reddit rechazan la lectura automatizada (HTTP 403), así que hay que
  consultarlas en un navegador. La necesidad de credenciales OAuth ya estaba documentada en
  [reddit_demo/](../reddit_demo/README.md).

Reddit se conserva como **ejemplo opcional** en `reddit_demo/`. Ningún archivo del caso principal lo importa.

## Archivos

| Archivo | Función |
|---|---|
| `etl_clima.py` | ETL compartido (solo biblioteca estándar): extraer, validar y limpiar, guardar histórico |
| `cron_clima.sh` | Envoltura para Cron con reintentos manuales (1 + 3) y registro en `resultados/clima_cron.log` |
| `flujo_clima_prefect.py` | El mismo ETL como `@flow` y `@task`, con reintentos automáticos y `serve` programado |

## Diseño del pipeline

1. **Extraer**: una petición a `https://api.open-meteo.com/v1/forecast` con las tres coordenadas,
   `current=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m` y
   `timezone=America/Mexico_City`. Se clasifican los errores: red, tiempo de espera, HTTP 429 y 5xx
   son **transitorios** (se reintentan); otros 4xx o JSON inesperado son **permanentes** (no se reintentan).
2. **Transformar**: se rechaza cualquier ciudad con valores ausentes, no numéricos o fuera de rango
   (temperatura −30 a 55 °C, humedad 0 a 100 %, precipitación 0 a 500 mm, viento 0 a 300 km/h).
   Las demás ciudades se conservan. Se redondea a un decimal.
3. **Guardar**: `resultados/clima_historico.csv` acumula el histórico. La clave
   `(ciudad, fecha_hora_local)` evita duplicados, y se escribe en un archivo temporal que luego
   reemplaza al original (`os.replace`). Columnas: fecha y hora local, zona horaria, ciudad,
   latitud y longitud de la celda del modelo que devuelve la API, cada variable con su unidad,
   fuente y momento de la consulta en UTC.

Como el dato "actual" cambia cada 15 minutos, dos ejecuciones dentro del mismo intervalo
producen las mismas filas, y la segunda las omite como duplicadas.

## Escenarios

La falla se **simula antes** de la petición. Cuando un intento no la simula, consulta la API real.

| Escenario | Intentos | Resultado |
|---|---|---|
| `normal` | 1 | Consulta real, datos guardados |
| `temporal` | 1 y 2 fallan con `ConnectionError`; el 3 consulta la API real | Datos guardados |
| `permanente` | Los 4 fallan | Sin datos; Cron sale con código 1 y Prefect queda en `Failed` |

```bash
bash clima_demo/cron_clima.sh temporal                          # Cron: reintentos manuales
python clima_demo/flujo_clima_prefect.py --escenario temporal  # Prefect: reintentos automáticos
python3 clima_demo/etl_clima.py                                 # un solo intento, sin reintentos
```

## Programación diaria a las 12:00 PM

**Zona horaria.** Colima usa la hora del centro (`America/Mexico_City`, UTC−6 todo el año desde
que se eliminó el horario de verano en 2022).
- **Cron** (Debian/Ubuntu) **no admite `CRON_TZ`** y usa la zona del sistema. Hay que comprobarla
  con `cat /etc/timezone` o `timedatectl`, y si no coincide, cambiarla con
  `sudo timedatectl set-timezone America/Mexico_City`.
- **Prefect** fija la zona en la propia programación: `Cron("0 12 * * *", timezone="America/Mexico_City")`.

**Cron.** Línea F de [`cron_demo/crontab.example`](../cron_demo/crontab.example). Se agrega sin
borrar tareas existentes:
```bash
P="/ruta/al/proyecto"
( crontab -l 2>/dev/null; echo "0 12 * * * /usr/bin/flock -n -E 99 \"$P/resultados/clima.lock\" /bin/bash \"$P/clima_demo/cron_clima.sh\" normal > /dev/null 2>> \"$P/resultados/clima_cron.error.log\"" ) | crontab -
crontab -l
```
Para quitarla, se filtra solo esa línea:
`crontab -l | grep -v 'clima_demo/cron_clima.sh' | crontab -`.

**Prefect.** Con `prefect server start` activo y `PREFECT_API_URL=http://127.0.0.1:4200/api`:
```bash
python clima_demo/flujo_clima_prefect.py --servir                 # 0 12 * * * America/Mexico_City
python clima_demo/flujo_clima_prefect.py --servir --cron "15 18 * * *"   # minuto cercano para probar
```
Las ejecuciones, los estados, los reintentos y los registros se ven en <http://127.0.0.1:4200>
(Runs → `etl-clima-colima`).

## Monitoreo

- **Cron**: `resultados/clima_cron.log` (cada intento, con fecha y zona horaria),
  `resultados/clima_cron.error.log` y el registro del servicio (`journalctl -u cron` o `/var/log/syslog`).
- **Prefect**: la interfaz web, o `prefect flow-run ls` y `prefect deployment ls`.

## Evidencias

Consultas reales del 25 de noviembre de 2026 en WSL. Ver los archivos 12 a 17 en
[evidencias/](../evidencias/README.md).
