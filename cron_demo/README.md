# Demostración con Cron

El mismo ETL del proyecto, en Python convencional (3.10 o superior) y sin Prefect.

| Archivo | Función |
|---|---|
| `demo_cron.py` | ETL; el subcomando `etl` hace un solo intento |
| `reintentos.sh` | Intento inicial + hasta 3 reintentos con 2 s de espera |
| `crontab.example` | Líneas para medianoche; todas vienen comentadas |

## Ejecución manual (Linux o WSL)

```bash
# Sin reintentos
python3 cron_demo/demo_cron.py etl --escenario normal      # código 0
python3 cron_demo/demo_cron.py etl --escenario temporal    # código 75, un solo intento

# Con reintentos manuales
bash cron_demo/reintentos.sh normal       # 1 intento,  código 0
bash cron_demo/reintentos.sh temporal     # 3 intentos, código 0
bash cron_demo/reintentos.sh permanente   # 4 intentos, código 1, sin CSV
echo $?                                   # código de la última ejecución
```

La versión sin reintentos también corre en PowerShell con `python cron_demo\demo_cron.py etl`.
Los CSV y registros quedan en `resultados/`, con un identificador distinto por ejecución.

## Cómo funcionan los reintentos

Cada intento es un proceso Python nuevo, así que una variable en memoria volvería a cero.
`reintentos.sh` crea una carpeta con `mktemp -d`, guarda ahí el contador y los datos
intermedios, y la borra con `trap` al terminar, también ante Ctrl+C o SIGTERM.
(Con SIGKILL ningún script puede limpiar.) `transformar` y `guardar` solo se invocan
si `extraer` terminó con código 0.

| Código | Significado |
|---|---|
| 0 | Éxito |
| 1 | `reintentos.sh`: reintentos agotados |
| 2 | Argumentos inválidos; no se reintenta |
| 75 | `demo_cron.py`: fallo de conexión de un intento |
| 99 | `flock`: ya hay otra ejecución activa |
| 130 / 143 | Interrumpido (Ctrl+C / SIGTERM) |

## Programar a medianoche

```text
0 0 * * * comando
│ │ │ │ └ día de la semana (0-7)
│ │ │ └── mes
│ │ └──── día del mes
│ └────── hora
└──────── minuto
```

1. Ajustar `PROYECTO` en `crontab.example` con una ruta absoluta y descomentar **una** línea.
2. Ejecutar `crontab -e` y pegar el contenido. `crontab archivo` también sirve, pero
   **reemplaza** todo el crontab del usuario.
3. Comprobar con `crontab -l`. Para quitarla, borrar la línea con `crontab -e`.

Las líneas usan intérpretes explícitos porque Cron no carga el perfil del usuario, y
separan la salida estándar (`cron.log`) de la de errores (`cron.error.log`). Los errores de cada
intento los registra `reintentos.sh` en su salida estándar y en su propio `.log`;
`cron.error.log` recibe los fallos del propio lanzador.

## Tarea diaria a las 12:00 PM (mediodía)

`12:00 PM` es mediodía (`0 12 * * *`); medianoche es `12:00 AM` (`0 0 * * *`). Cron usa la zona
horaria del sistema, que se consulta con `timedatectl` o `date`.

1. Ajustar `PROYECTO` y descomentar solo la línea **D** de `crontab.example`.
2. Instalarla sin borrar otras tareas del usuario:
   ```bash
   crontab -l 2>/dev/null > /tmp/crontab.respaldo        # respaldo
   crontab -e                                            # pegar variables y línea D
   ```
3. Comprobar la configuración:
   ```bash
   crontab -l | grep '^0 12'           # la tarea quedó registrada
   service cron status                 # el servicio está activo (en WSL: sudo service cron start)
   ```
4. Comprobar la ejecución después de las 12:00:
   ```bash
   grep CRON /var/log/syslog | tail    # o: journalctl -u cron --since today
   tail resultados/cron.log            # "Completado en N intento(s)"
   ls -t resultados/*.csv | head -1    # CSV nuevo con total,120
   ```
   Para no esperar al mediodía, se puede probar temporalmente con `*/2 * * * *` (cada 2 minutos)
   y volver a `0 12 * * *` al terminar.
5. Desactivarla: `crontab -e` y borrar o comentar la línea; restaurar el respaldo con
   `crontab /tmp/crontab.respaldo` si hace falta.

## Evitar solapamientos con `flock`

Cron inicia la tarea aunque la anterior siga activa. `flock -n` falla de inmediato si otro
proceso tiene el candado; `-E 99` distingue ese caso de un error del ETL:

```bash
flock -n -E 99 resultados/etl.lock bash cron_demo/reintentos.sh temporal
```

## Desde Windows

Cron no existe de forma nativa en PowerShell ni en Windows. Para probarlo con WSL:

```powershell
wsl --install -d Ubuntu     # solo si no hay una distribución
wsl -d Ubuntu
```

```bash
sudo apt install cron
service cron status
```

Conviene clonar el proyecto dentro de Linux (`~/`), no en `/mnt/c`. Cron corre únicamente
mientras la distribución de WSL está encendida; si el equipo está apagado a medianoche,
esa ejecución se pierde. El equivalente nativo de Windows es el Programador de tareas.

## Caso Open-Meteo
La línea F de `crontab.example` programa el ETL real de clima (`clima_demo/cron_clima.sh`) a las 12:00 PM, con la zona horaria del sistema. Instalación sin borrar otras tareas y prueba efectiva: [clima_demo/README.md](../clima_demo/README.md).
