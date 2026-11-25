---
title: "Orquestación de procesos de datos: Cron y Prefect"
subtitle: "Ingeniería de Datos · Unidad III · Sesión 49 · Reporte técnico individual"
lang: es
papersize: us-letter
fontsize: 10pt
margin:
  x: 2cm
  y: 1.7cm
---

## Introducción

Un proceso ETL que corre todas las noches debe iniciar a tiempo y también responder cuando algo falla. Este reporte compara Cron, el programador clásico de Linux, con Prefect, un orquestador para Python. Para hacerlo se programó el mismo ETL en ambas herramientas: extrae `[10, 20, 30]`, lo transforma en `[20, 40, 60]` y guarda un CSV con total 120. Se ejecutó en Ubuntu 24.04 (WSL 2) con Python 3.12.3 y Prefect 3.9.5.

## 1. Comparativa funcional

Cron inicia un comando en la fecha indicada por cinco campos; `0 0 * * *` significa todos los días a medianoche. No sabe qué hace el comando ni cómo se relacionan sus pasos. En Prefect, cada paso es una tarea (`@task`) y el flujo (`@flow`) las coordina, de modo que la herramienta conoce el estado de cada una.

| Aspecto | Cron | Prefect |
|:-----------------|:-----------------------------|:-----------------------------|
| Programación | Expresión de cinco campos | Cron o intervalo, con `serve` |
| Dependencias | Se programan a mano | Resultados entre tareas |
| Errores y reintentos | Código de salida; sin reintentos | Estado Failed; reintentos por tarea |
| Monitoreo y registros | Correo o archivos redirigidos | Interfaz web y registro por tarea |
| Solapamientos | Requiere `flock` | `limit=1` en `serve` (por proceso) |
| Infraestructura | Servicio cron | Python, servidor y proceso activo |
| Uso recomendado | Tareas simples y aisladas | Flujos con varias tareas |

## 2. Análisis de contingencia: desconexión a medianoche

Si la conexión falla cuando inicia la extracción, **una tarea de Cron sin reintentos termina con error y no vuelve a ejecutarse hasta la medianoche siguiente**, salvo que alguien la lance a mano. Los datos quedan con un día de retraso y el problema puede pasar inadvertido si nadie revisa el correo o los registros. Con reintentos, manuales o de Prefect, el proceso espera e intenta de nuevo; si todos los intentos fallan, se detiene sin transformar ni guardar. Los resultados medidos fueron:

| Escenario | Cron sin reintentos | Cron con reintentos | Prefect |
|:-----------|:-------------------|:-------------------|:----------------------|
| Normal | 1 intento, código 0 | 1 intento, código 0 | 1 intento, Completed |
| Temporal | 1 intento, código 75 | 3 intentos, código 0 | 3 intentos, Completed |
| Permanente | 1 intento, código 75 | 4 intentos, código 1 | 4 intentos, Failed |

Cuando hubo éxito, el CSV sumó 120; en los fallos definitivos no se generó archivo ni se ejecutaron las tareas dependientes. Las 15 pruebas automatizadas se aprobaron en Linux. La línea de Cron se probó con un entorno mínimo equivalente, sin instalarla ni esperar a medianoche. Cabe señalar que tres esperas de dos segundos solo cubren unos seis segundos de desconexión: en producción se necesitan esperas mayores, por ejemplo `retry_delay_seconds=[60, 300, 900]`.

## 3. Reintentos: manuales en Cron y declarativos en Prefect

Cron no reintenta, así que debe llamar a un script que repita la extracción y revise su código de salida. Esta es la idea central, simplificada, de `reintentos.sh`:

```bash
n=1
until python3 demo_cron.py extraer --estado "$ESTADO/intentos" ...; do
  [ "$n" -ge 4 ] && exit 1     # agotados: no se ejecutan las dependientes
  sleep 2; n=$((n + 1))
done
python3 demo_cron.py transformar ... && python3 demo_cron.py guardar ...
```

Como cada intento es un proceso distinto, el contador se guarda en una carpeta temporal propia de la ejecución, que se borra al terminar. También hubo que programar los registros. Para evitar que dos ejecuciones coincidan, la línea de Cron usa `flock`:

```bash
0 0 * * * /usr/bin/flock -n -E 99 /ruta/etl.lock /bin/bash /ruta/reintentos.sh
```

En Prefect, la política completa de reintentos se declara en una sola línea sobre la función:

```python
@task(retries=3, retry_delay_seconds=2)
def extraer():
    ...
```

`retries=3` indica tres reintentos **adicionales**: con el intento inicial son **hasta cuatro ejecuciones**, separadas por dos segundos. Así se comprobó en el escenario permanente. Prefect lleva el conteo, registra cada intento y no inicia las tareas que dependen del resultado fallido.

## Conclusión

Ambas opciones se recuperaron del fallo temporal con el mismo número de intentos; la diferencia está en quién hace el trabajo. Con Cron, la recuperación, el estado y los registros son código propio que se debe probar y mantener. Con Prefect se declaran, a cambio de mantener un servidor y un proceso activos. Cron es suficiente para tareas sencillas; Prefect conviene cuando hay dependencias y se necesita saber qué falló y en qué paso.

## Referencias

- Proyecto cronie. (s. f.). *crontab(5)*. Linux manual pages. <https://man7.org/linux/man-pages/man5/crontab.5.html>
- Proyecto util-linux. (s. f.). *flock(1)*. Linux manual pages. <https://man7.org/linux/man-pages/man1/flock.1.html>
- Prefect. (s. f.). *Prefect documentation*. <https://docs.prefect.io/>
