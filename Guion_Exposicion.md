# Guion de exposición — Orquestación moderna con Prefect en Python
**Equipo 3 · Ingeniería de Datos · Grupo LCS2** · Exposición: 300 s (5 min) · Preguntas: 3 min

**Caso práctico:** pipeline ETL que, todos los días a las 12:00 (hora del centro de México), recopila el clima **real** de Colima, Villa de Álvarez y Manzanillo desde la API pública Open-Meteo. Las desconexiones de los escenarios de fallo son **simuladas**; los datos, no.

| # | Diapositiva | Integrante | Tiempo | Acumulado |
|---|---|---|---|---|
| 1 | ¿Qué es Prefect? y caso práctico | Integrante 1 | 30 s | 0:30 |
| 2 | ¿Cómo funciona Prefect? (ETL del clima) | Integrante 2 | 35 s | 1:05 |
| 3 | De funciones a flujos (`@task`) | Integrante 3 | 35 s | 1:40 |
| 3 | De funciones a flujos (`@flow`) | Integrante 4 | 35 s | 2:15 |
| 4 | Recuperación automática ante fallos | Integrante 5 | 40 s | 2:55 |
| 5 | Monitoreo | Integrante 6 | 35 s | 3:30 |
| 5 | Demostración en vivo (escenario temporal) | Integrante 7 | 60 s | 4:30 |
| 5 | Interpretación y conclusión | Integrante 8 | 30 s | **5:00** |
| | **Total** | **8 integrantes** | **300 s** | |

Guía paso a paso de la demostración (ventanas, comandos y resultados esperados): [docs/GUIA_DEMO_EN_VIVO.md](docs/GUIA_DEMO_EN_VIVO.md).

---

## Diapositiva 1 — Integrante 1 (30 s)
"Buen día. Somos el Equipo 3 de Ingeniería de Datos, grupo LCS2, y hoy presentamos la orquestación moderna con Prefect en Python.
Prefect es un framework de Python para orquestar flujos de trabajo: convierte funciones normales en procesos que se ejecutan en orden, que se recuperan solos ante errores temporales y que podemos monitorear.
Lo aplicamos a un caso real: cada día a las 12:00 recopilamos el clima de Colima, Villa de Álvarez y Manzanillo desde la API pública Open-Meteo. Automatización, gestión de errores y visibilidad."

**Transición:** "Para entender cómo lo logra, (integrante 2) nos explicará cómo está armado este flujo."

## Diapositiva 2 — Integrante 2 (35 s)
"Nuestro pipeline es un ETL de tres tareas. Extraer consulta Open-Meteo y obtiene temperatura, humedad, precipitación y viento de las tres ciudades; transformar valida los rangos y conserva las unidades; y guardar agrega las filas a un CSV histórico sin duplicados.
Cada paso es una tarea, marcada con `@task`, y el flujo `etl-clima-colima`, marcado con `@flow`, es el contenedor punteado. Las flechas son dependencias: transformar recibe el JSON de extraer y guardar recibe las filas. Si extraer falla, Prefect no ejecuta las siguientes, y cada tarea registra su propio estado."

**Transición:** "Ahora veamos cómo se escribe en código. Les dejo con (integrante 3)."

## Diapositiva 3 — Integrante 3 (35 s) · `@task`
"Este es el fragmento esencial de nuestro flujo, simplificado. Empecemos por el decorador `@task`, resaltado en azul. Al ponerlo sobre la función `extraer`, Prefect la convierte en una tarea: una operación individual y concreta, en este caso consultar Open-Meteo mediante nuestro módulo `etl`.
Sigue siendo una función normal de Python, pero ahora Prefect registra su estado, su resultado y cada uno de sus intentos."

**Transición:** "Pero una tarea sola no forma un proceso; para eso está el flujo, que explicará (integrante 4)."

## Diapositiva 3 — Integrante 4 (35 s) · `@flow`
"El decorador `@flow` convierte `flujo_clima` en el coordinador del proceso y le da el nombre que veremos en la interfaz: `etl-clima-colima`. Dentro, `datos = extraer()` recibe el resultado de la tarea y lo pasa a transformar y luego a guardar; ese paso de resultados es lo que crea las dependencias. La última línea ejecuta el flujo.
En el proyecto, este mismo flujo se programa todos los días a las 12:00 con `serve` y `Cron("0 12 * * *", timezone="America/Mexico_City")`."

**Transición:** "¿Y qué pasa si la consulta falla? (Integrante 5) nos lo explica."

## Diapositiva 4 — Integrante 5 (40 s)
"Extraer depende de internet. Si la conexión se cae justo al consultar la API, sin orquestación perderíamos el dato del día.
En nuestro código basta con `@task(retries=3, retry_delay_seconds=5)`: hasta tres reintentos adicionales, con cinco segundos de espera.
En la línea de tiempo simulamos dos desconexiones: el intento inicial falla, Prefect espera cinco segundos, el reintento 1 falla, otros cinco segundos, y en el reintento 2 la consulta real devuelve las tres ciudades. El flujo continúa con transformar y guardar.
Abajo están los resultados reales: normal, un intento; temporal, tres; permanente, cuatro intentos y Failed, sin guardar nada. Con Cron, la misma política exigió un script Bash de unas 35 líneas; aquí son dos parámetros."

**Transición:** "¿Y cómo sabemos lo que pasó en cada intento? (Integrante 6) nos muestra el monitoreo."

## Diapositiva 5 — Integrante 6 (35 s) · Monitoreo
"Prefect registra el estado de cada flujo y de cada tarea: Pending, cuando espera ejecutarse; Running, mientras se ejecuta; AwaitingRetry, cuando falló y espera su reintento; Completed, cuando termina con éxito; y Failed, cuando ya no quedan reintentos.
A la derecha está el registro real de la tarea extraer en la Prefect UI, en el escenario temporal: dos ConnectionError simulados, 'Retry 1/3' y 'Retry 2/3' con cinco segundos de espera, y en el intento 3 se reciben las tres ubicaciones. Ahora lo verán en vivo."

**Transición:** "(Integrante 7) lo va a ejecutar en este momento."

## Diapositiva 5 — Integrante 7 (60 s) · Demostración en vivo
"En el navegador tenemos la Prefect UI. En el historial ya están dos ejecuciones previas: el escenario normal, en verde, y el permanente, en rojo, con cuatro intentos fallidos.
Ahora ejecuto el escenario temporal: `python clima_demo/flujo_clima_prefect.py --escenario temporal`. *(Ejecutar.)*
En la terminal: intento 1, ConnectionError, y Prefect programa el 'Retry 1/3' en cinco segundos; intento 2, otro error, 'Retry 2/3'; e intento 3: 'Extraer: 3 ubicaciones recibidas'. Transformar imprime la temperatura, la humedad, la lluvia y el viento reales de cada ciudad, guardar agrega las filas al histórico y el estado final es COMPLETED.
*(Actualizar la UI.)* El nuevo flujo aparece en verde y, en la tarea extraer, Run Count 3."

**Plan B:** si la terminal o la red fallan, mostrar el registro real de la diapositiva 5 o las capturas del reporte (figuras 6 y 7).

**Transición:** "Para cerrar, (integrante 8) interpreta lo que acabamos de ver."

## Diapositiva 5 — Integrante 8 (30 s) · Conclusión
"Lo que vimos demuestra tres ideas: `@flow` y `@task` coordinan el ETL en el orden correcto; `retries` recuperó la consulta ante una desconexión temporal; y los estados y registros nos dicen qué pasó en cada intento. Cron solo decide cuándo empezar; Prefect administra también lo que ocurre después.
En conclusión: **Prefect no solo ejecuta procesos: también permite coordinarlos, recuperarlos y monitorearlos.** Gracias; abrimos el espacio de preguntas."

---

## Posibles preguntas (3 min)
- **¿`retries=3` significa 3 ejecuciones?** No: 1 intento inicial + 3 reintentos = hasta 4 ejecuciones (escenario permanente).
- **¿Los datos son reales?** Sí, cada consulta exitosa es una petición real a Open-Meteo. Solo las desconexiones se simulan, antes de la petición, para poder reproducirlas.
- **¿Por qué Open-Meteo y no Reddit?** La API de Reddit requiere credenciales OAuth de las que el equipo no dispone; Open-Meteo no pide clave para uso no comercial.
- **¿Qué pasa si se agotan los reintentos?** La tarea y el flujo terminan en Failed, y transformar y guardar no se ejecutan.
- **¿Se duplican datos si se repite la ejecución?** No: el CSV usa la clave ciudad + fecha y hora; una segunda ejecución en el mismo intervalo de 15 minutos reporta "duplicadas omitidas".
- **¿Y la zona horaria?** Prefect la declara en la programación (`America/Mexico_City`); Cron de Ubuntu usa la del sistema.
- **¿Diferencia entre tarea y flujo?** La tarea es una unidad de trabajo; el flujo coordina tareas y es lo que se ejecuta y programa.
- **¿Se puede usar una espera distinta por reintento?** Sí: `retry_delay_seconds` acepta una lista, p. ej. `[60, 300, 900]`.

La demostración base sin internet (`python demo_prefect.py`, datos `[10, 20, 30]`) se conserva en el repositorio como respaldo.
