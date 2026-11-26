# Guía de demostración en vivo — ETL del clima con Prefect

Corresponde a la diapositiva 5 (integrantes 6, 7 y 8). **Demostración principal:** escenario `temporal`,
ejecutado en vivo. Los escenarios `normal` y `permanente` se ejecutan **antes** de la exposición para
que su historial ya aparezca en la Prefect UI.

| | |
|---|---|
| Duración en vivo | 60 s (integrante 7); la ejecución tarda ≈ 11 s |
| Requisitos | Linux o WSL con el entorno virtual del proyecto (Prefect 3), **internet** (el intento 3 consulta Open-Meteo de verdad) y un navegador |
| Datos | Reales (Open-Meteo). Solo las dos desconexiones del escenario temporal son simuladas |

## 1. Ventanas que deben estar abiertas

| Ventana | Contenido | Estado al empezar la exposición |
|---|---|---|
| **A. Terminal del servidor** | `prefect server start` | En ejecución, minimizada. No se toca |
| **B. Terminal de la demo** | Carpeta del proyecto, entorno activo, `PREFECT_API_URL` definida | Comando del escenario temporal ya escrito, sin ejecutar. Letra grande (Ctrl + +) |
| **C. Navegador** | Prefect UI → **Runs** (`http://127.0.0.1:4200/runs`) | Muestra las dos ejecuciones previas (normal Completed y permanente Failed) |
| **D. PowerPoint** | Presentación en modo presentador | En la diapositiva 5 al llegar al integrante 6 |

Para cambiar de ventana durante la demo: Alt + Tab entre D → B → C → D.

## 2. Preparación (15 minutos antes)

Terminal A (Linux/WSL):
```bash
cd <carpeta del proyecto>/Sesion49_Equipo3_Prefect
source venv/bin/activate              # entorno donde está instalado Prefect
prefect server start                  # esperar el mensaje con http://127.0.0.1:4200
```

Terminal B:
```bash
cd <carpeta del proyecto>/Sesion49_Equipo3_Prefect
source venv/bin/activate
export PREFECT_API_URL=http://127.0.0.1:4200/api
curl -s https://api.open-meteo.com/v1/forecast?latitude=19.24\&longitude=-103.72\&current=temperature_2m | head -c 80; echo   # comprobar internet

# Historial previo (una sola vez, antes de exponer)
python clima_demo/flujo_clima_prefect.py --escenario normal       # ≈ 2 s · Estado final: COMPLETED
python clima_demo/flujo_clima_prefect.py --escenario permanente   # ≈ 16 s · Estado final: FAILED (código 1, es lo esperado)
clear
```

En Windows PowerShell, la variable se define con `$env:PREFECT_API_URL = "http://127.0.0.1:4200/api"`.

Navegador (C): abrir `http://127.0.0.1:4200/runs`. Si aparece el aviso *Join the Prefect Community*,
pulsar **Skip**; si aparece el banner *V1 UI will be removed*, pulsar **Dismiss**. Comprobar que se ven
dos ejecuciones de `etl-clima-colima`: una **Failed** (roja) y una **Completed** (verde).

Por último, escribir en la terminal B, **sin pulsar Enter**:
```bash
python clima_demo/flujo_clima_prefect.py --escenario temporal
```

## 3. Guion de la demostración (integrante 7, 60 s)

| Tiempo | Acción | Qué se ve | Qué explicar |
|---|---|---|---|
| 0–10 s | Mostrar el navegador (C) | Runs: normal **Completed**, permanente **Failed** | "Ya ejecutamos dos escenarios: normal terminó en un intento; permanente falló cuatro veces y quedó en Failed sin guardar datos." |
| 10–15 s | Pasar a la terminal (B) y pulsar **Enter** | `Beginning flow run ... for flow 'etl-clima-colima'` | "Ahora el escenario temporal: simulamos dos desconexiones antes de consultar la API." |
| 15–20 s | Esperar | `Extraer Open-Meteo: intento 1` y `ConnectionError(... intento 1) - Retry 1/3 will start 5 second(s) from now` | "Primer fallo: Prefect no se detiene, programa el reintento 1 de 3 en cinco segundos." |
| 20–25 s | Esperar | `intento 2` y `Retry 2/3 will start 5 second(s) from now` | "Segundo fallo, segundo reintento." |
| 25–35 s | Esperar | `intento 3`, `Extraer: 3 ubicaciones recibidas`, tres líneas de transformar con °C, %, mm y km/h | "En el intento 3 la consulta real a Open-Meteo funciona: estos son los datos de ahora mismo en Colima, Villa de Álvarez y Manzanillo." |
| 35–45 s | Esperar | `Guardar: N filas nuevas, M duplicadas omitidas` y `Estado final: COMPLETED` | "Guardar agrega las filas al histórico; si ya existían de una ejecución anterior en el mismo intervalo de 15 min, las omite. El flujo termina en Completed." |
| 45–60 s | Pasar al navegador (C) y recargar (F5) → abrir la nueva ejecución → tarea `extraer` | Nueva fila **Completed** arriba; en la tarea, **Run Count 3**, **Retries 3**, **Retry Delay 5s** y los mismos mensajes de reintento | "Todo lo que vimos en la terminal queda registrado: estado, intentos y registros por tarea." |

**Salida esperada en la terminal** (ejecución real del 25/11/2026; la hora, los valores y el nombre aleatorio del flujo cambian):
```text
INFO | Flow run 'glaring-phoenix' - Beginning flow run 'glaring-phoenix' for flow 'etl-clima-colima'
INFO | Task run 'extraer-eb5' - Extraer Open-Meteo: intento 1 (escenario=temporal)
INFO | Task run 'extraer-eb5' - Task run failed with exception: ConnectionError('Desconexión simulada (escenario=temporal, intento 1)') - Retry 1/3 will start 5 second(s) from now
INFO | Task run 'extraer-eb5' - Extraer Open-Meteo: intento 2 (escenario=temporal)
INFO | Task run 'extraer-eb5' - Task run failed with exception: ConnectionError('Desconexión simulada (escenario=temporal, intento 2)') - Retry 2/3 will start 5 second(s) from now
INFO | Task run 'extraer-eb5' - Extraer Open-Meteo: intento 3 (escenario=temporal)
INFO | Task run 'extraer-eb5' - Extraer: 3 ubicaciones recibidas
INFO | Task run 'transformar-8d4' -   2026-11-25T23:00 Colima: 23.3 °C, 94.0 %, 0.0 mm, 0.6 km/h
INFO | Task run 'transformar-8d4' -   2026-11-25T23:00 Villa de Álvarez: 22.6 °C, 89.0 %, 0.0 mm, 1.2 km/h
INFO | Task run 'transformar-8d4' -   2026-11-25T23:00 Manzanillo: 25.2 °C, 86.0 %, 0.0 mm, 7.6 km/h
INFO | Task run 'guardar-b8f' - Guardar: 0 filas nuevas, 3 duplicadas omitidas, 6 en clima_historico.csv
INFO | Flow run 'glaring-phoenix' - Finished in state Completed()
Estado final: COMPLETED
```

## 4. Integrantes 6 y 8

- **Integrante 6 (antes de la demo):** señala en la diapositiva 5 los cinco estados y el registro real
  de la derecha; anuncia que se verá lo mismo en vivo.
- **Integrante 8 (después):** puede dejar visible la Prefect UI mientras concluye: "El normal se completó
  en un intento, el temporal se recuperó en el tercero y el permanente quedó en Failed sin guardar datos."

## 5. Plan B

| Problema | Qué hacer |
|---|---|
| Sin internet | El intento 3 también fallaría (error de red real), y el escenario terminaría en Failed tras 4 intentos. No ejecutar en vivo: mostrar el registro real de la diapositiva 5 y, si hay tiempo, `python demo_prefect.py` (demostración base sin internet) |
| El servidor no responde | Ejecutar el escenario sin servidor: abrir otra terminal sin `PREFECT_API_URL` y lanzar el mismo comando (Prefect usa un servidor temporal; la terminal muestra lo mismo, pero no se verá en la UI) |
| `Timed out while attempting to connect to ephemeral Prefect API server` | `export PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS=120` y repetir |
| La demo se alarga | Cortar en cuanto aparezca `Estado final: COMPLETED` y omitir la vista de la tarea en la UI |
| Falla todo | Usar las capturas reales del reporte (figuras 3 a 9) |

## 6. Después de la exposición

`Ctrl + C` en la terminal A detiene el servidor. Las ejecuciones quedan guardadas en la base local de
Prefect y el CSV en `resultados/clima_historico.csv` (no versionado).
