# Guion de exposición — Orquestación moderna con Prefect en Python
**Equipo 3 · Ingeniería de Datos · Grupo LCS2** · Exposición: 300 s (5 min) · Preguntas: 3 min

| # | Diapositiva | Integrante | Tiempo | Acumulado |
|---|---|---|---|---|
| 1 | ¿Qué es Prefect? | Integrante 1 | 30 s | 0:30 |
| 2 | ¿Cómo funciona Prefect? | Integrante 2 | 35 s | 1:05 |
| 3 | De funciones a flujos (`@task`) | Integrante 3 | 35 s | 1:40 |
| 3 | De funciones a flujos (`@flow`) | Integrante 4 | 35 s | 2:15 |
| 4 | Recuperación automática ante fallos | Integrante 5 | 40 s | 2:55 |
| 5 | Monitoreo | Integrante 6 | 35 s | 3:30 |
| 5 | Demostración en terminal | Integrante 7 | 60 s | 4:30 |
| 5 | Interpretación y conclusión | Integrante 8 | 30 s | **5:00** |
| | **Total** | **8 integrantes** | **300 s** | |

---

## Diapositiva 1 — Integrante 1 (30 s)
"Buen día. Somos el Equipo 3 de Ingeniería de Datos, grupo LCS2, y hoy presentamos la orquestación moderna con Prefect en Python.
Prefect es un framework de Python para orquestar flujos de trabajo. Toma funciones normales y las convierte en procesos que se ejecutan de forma automática y en orden, que se recuperan solos ante errores temporales y que podemos monitorear en cada ejecución. Es decir: automatización, gestión de errores y visibilidad."

**Transición:** "Para entender cómo lo logra, (integrante 2) nos explicará cómo funciona un flujo en Prefect."

## Diapositiva 2 — Integrante 2 (35 s)
"Un proceso ETL típico tiene tres pasos: extraer, transformar y cargar. En Prefect, cada paso se escribe como una tarea, marcada con `@task`, y las tres se agrupan dentro de un flujo, marcado con `@flow`: el contenedor punteado.
Las flechas muestran la dependencia: transformar no puede empezar sin los datos que devuelve extraer, y cargar necesita los datos ya limpios. Prefect detecta esa dependencia porque el resultado de una tarea se pasa como argumento a la siguiente. Además, cada tarea tiene su propio estado, así sabemos qué paso funcionó y cuál falló."

**Transición:** "Ahora veamos cómo se escribe en código. Les dejo con (integrante 3)."

## Diapositiva 3 — Integrante 3 (35 s) · `@task`
"Este es un programa completo y funcional. Empecemos por el decorador `@task`, resaltado en azul. Al ponerlo sobre la función `extraer`, Prefect la convierte en una tarea: una operación individual y concreta, en este caso devolver la lista 10, 20, 30. Sigue siendo Python normal, pero ahora Prefect registra su estado y su resultado cada vez que se ejecuta."

**Transición:** "Pero una tarea sola no forma un proceso; para eso está el flujo, que explicará (integrante 4)."

## Diapositiva 3 — Integrante 4 (35 s) · `@flow`
"El decorador `@flow` convierte la función `pipeline` en un flujo: el coordinador del proceso. Dentro del flujo llamamos a la tarea con `datos = extraer()`; así el flujo recibe el resultado y decide qué hacer después, aquí imprimirlo. La última línea, `pipeline()`, ejecuta todo. No son dos fragmentos separados: la tarea hace el trabajo y el flujo la coordina, en el mismo archivo."

**Transición:** "¿Y qué pasa si la extracción falla? (Integrante 5) nos lo explica."

## Diapositiva 4 — Integrante 5 (40 s)
"Imaginemos que la extracción depende de internet y la conexión se cae un momento. Sin orquestación, el programa simplemente fallaría.
Con Prefect basta con configurar `@task(retries=3, retry_delay_seconds=2)`: hasta tres reintentos adicionales, con dos segundos de espera entre cada uno.
En la línea de tiempo: el intento inicial falla (rojo); Prefect espera dos segundos (ámbar); el reintento 1 vuelve a fallar; otros dos segundos; y en el reintento 2 la conexión ya volvió y la extracción tiene éxito (verde). El flujo continúa con transformar y cargar.
Tres reintentos más el intento inicial dan hasta cuatro ejecuciones. Si las cuatro fallaran, la tarea quedaría en estado Failed."

**Transición:** "¿Y cómo sabemos lo que pasó en cada intento? (Integrante 6) nos muestra el monitoreo."

## Diapositiva 5 — Integrante 6 (35 s) · Monitoreo
"Prefect registra el estado de cada flujo y de cada tarea: Pending, cuando espera ejecutarse; Running, mientras se ejecuta; AwaitingRetry, cuando falló y espera su reintento; Completed, cuando termina con éxito; y Failed, cuando el error es definitivo porque ya no quedan reintentos.
A la derecha vemos una simulación ilustrativa del registro: dos errores de conexión, sus esperas y el tercer intento exitoso, seguido de transformar y guardar. Estos estados también pueden consultarse en la interfaz web de Prefect."

**Transición:** "Ahora (integrante 7) lo va a ejecutar en vivo."

## Diapositiva 5 — Integrante 7 (60 s) · Demostración
"Voy a ejecutar `python demo_prefect.py`. *(Ejecutar.)* El programa simula una desconexión en los dos primeros intentos, sin depender de internet. Observen la terminal: primer `ConnectionError` y Prefect programa el reintento; dos segundos; segundo error; dos segundos más; y en el tercer intento la extracción funciona.
Después se ejecutan transformar y guardar, se imprime el resultado final —`[20, 40, 60]`, total 120— y el flujo termina en Completed."

**Plan B:** si la terminal falla, explicar el registro simulado de la diapositiva.

**Transición:** "Para cerrar, (integrante 8) interpreta lo que acabamos de ver."

## Diapositiva 5 — Integrante 8 (30 s) · Conclusión
"Lo que vimos demuestra tres ideas: Prefect coordina las tareas en el orden correcto con `@flow` y `@task`; las recupera automáticamente ante errores temporales con `retries`; y nos deja monitorear cada intento con sus estados y registros.
En conclusión: **Prefect no solo ejecuta procesos: también permite coordinarlos, recuperarlos y monitorearlos.** Gracias; abrimos el espacio de preguntas."

---

## Demostración: instalación y ejecución
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate   |   macOS/Linux: source .venv/bin/activate
pip install -U prefect
python demo_prefect.py
```
- Requiere Python 3.11 o superior (exigencia de Prefect 3). No necesita internet para simular el fallo (solo para instalar Prefect).
- Opcional, para ver los estados en la interfaz web: en otra terminal `prefect server start` y abrir `http://127.0.0.1:4200`.
- **Ensayen la demo antes de exponer** en el equipo que se usará: la primera ejecución de Prefect puede tardar unos segundos extra en iniciar.

**Salida esperada de las impresiones del programa** (Prefect añade además sus propias líneas de log con fecha, nivel y estado, cuyo formato exacto depende de la versión):
```text
Extraer: intento 1
Extraer: intento 2
Extraer: intento 3
Extraer: conexión recuperada en el intento 3 -> [10, 20, 30]
Transformar: [10, 20, 30] -> [20, 40, 60]
Guardar: 3 registros guardados (total = 120)
Resultado final: [20, 40, 60] | total = 120
```

## Posibles preguntas (3 min)
- **¿`retries=3` significa 3 ejecuciones?** No: 1 intento inicial + 3 reintentos = hasta 4 ejecuciones.
- **¿Se puede usar una espera distinta por reintento?** Sí: `retry_delay_seconds` acepta también una lista, p. ej. `[1, 2, 4]`.
- **¿Qué pasa si se agotan los reintentos?** La tarea termina en estado Failed y el flujo también falla (salvo que se maneje el error).
- **¿Diferencia entre tarea y flujo?** La tarea es una unidad de trabajo; el flujo coordina tareas y es lo que se ejecuta.
