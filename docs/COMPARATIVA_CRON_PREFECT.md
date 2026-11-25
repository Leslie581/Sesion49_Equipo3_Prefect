# Comparativa técnica: Cron y Prefect

Las cifras de intentos y códigos provienen de las pruebas en [evidencias/](../evidencias/README.md)
(Ubuntu 24.04 en WSL 2, Python 3.12.3, Prefect 3.9.5).

## Comparación general

| Aspecto | 1. Cron sin reintentos | 2. Cron + `reintentos.sh` | 3. Prefect |
|---|---|---|---|
| Programación temporal | `0 0 * * *`, resolución de un minuto | Igual | `serve(cron="0 0 * * *")` o `interval=60` |
| Dependencias entre tareas | Las que escriba el programa | Orden y códigos comprobados en Bash | El flujo pasa el resultado de una tarea a la siguiente |
| Manejo de errores | Código de salida; nadie reacciona | Código comprobado en cada etapa | Excepción convertida en estado Failed |
| Reintentos | Ninguno | Bucle, espera y contador propios | `@task(retries=3, retry_delay_seconds=2)` |
| Monitoreo | Correo (`MAILTO`) o archivos | Archivos | Interfaz web y estados por tarea |
| Registros | Redirección manual | `.log` por ejecución | Automáticos, por flujo y tarea |
| Solapamientos | No los evita; requiere `flock` | `flock -n -E 99` | `limit=1` en `serve`, para ese proceso |
| Complejidad | Mínima | Media: código propio que mantener | Baja en código; requiere conocer la herramienta |
| Infraestructura | Servicio cron | Servicio cron y Bash | Python ≥ 3.11, Prefect, servidor y proceso `serve` activo |
| Uso recomendado | Tareas simples que toleran un fallo | Pocas tareas en un servidor Linux | Flujos con varias tareas, recuperación y seguimiento |

El manual de `crontab(5)` (cronie) describe cuándo se ejecuta un comando y cómo se envía su
salida; no define reintentos ni control de solapamientos.

## Resultados por escenario

| Escenario | Cron sin reintentos | Cron con reintentos | Prefect |
|---|---|---|---|
| normal | 1 intento, código 0, total 120 | 1 intento, código 0, total 120 | 1 intento, Completed, total 120 |
| temporal | 1 intento, código 75, sin CSV | 3 intentos, código 0, total 120 | 3 intentos, Completed, total 120 |
| permanente | 1 intento, código 75, sin CSV | 4 intentos, código 1, sin CSV | 4 intentos, Failed, código 1, sin CSV |

## ¿Qué pasa si a medianoche se cae internet?

1. **Cron sin reintentos.** El proceso termina con error. Cron no vuelve a intentarlo y la
   siguiente oportunidad llega 24 horas después, a menos que alguien lo ejecute a mano. El aviso
   depende de que exista correo configurado o de que alguien lea los registros.
2. **Cron con reintentos.** El script espera 2 s y vuelve a intentar, hasta cuatro ejecuciones.
   Si la conexión regresa, continúa; si no, termina con código 1 sin transformar ni guardar.
3. **Prefect.** Aplica la misma política con un decorador. La tarea pasa por AwaitingRetry y
   Retrying, y cada intento queda registrado. Si se agotan, la tarea y el flujo quedan en Failed.

**Límite común:** con tres esperas de 2 s solo se cubren unos 6 segundos de desconexión. Es un
valor útil para la demostración. En producción conviene una espera mayor o creciente, por ejemplo
`retry_delay_seconds=[60, 300, 900]`, que cubre unos 21 minutos.

**Además:** ninguna opción se ejecuta a tiempo si el equipo está apagado, y en Prefect el proceso
con `serve` debe seguir activo. Si los datos se escriben en un destino externo, los reintentos
exigen que la operación sea idempotente; aquí el CSV se escribe en un solo paso con `os.replace`.

## Conclusión

Cron decide **cuándo** iniciar un comando. Prefect también administra **qué ocurre después**:
dependencias, reintentos, estados y registros. Cron con Bash puede igualar los reintentos, pero
`reintentos.sh` necesita más de 60 líneas para lo que en Prefect son dos parámetros. A cambio,
Prefect requiere instalar y mantener más componentes; para una tarea aislada, Cron suele bastar.

## Referencias

- Prefect: <https://docs.prefect.io/> ([reintentos](https://docs.prefect.io/v3/how-to-guides/workflows/retries), [instalación](https://docs.prefect.io/v3/get-started/install), [procesos locales](https://docs.prefect.io/v3/how-to-guides/deployment_infra/run-flows-in-local-processes))
- `crontab(5)`: <https://man7.org/linux/man-pages/man5/crontab.5.html>
- `flock(1)`: <https://man7.org/linux/man-pages/man1/flock.1.html>
