#!/usr/bin/env bash
# Regenera evidencias/ con ejecuciones reales (Linux o WSL).
# Uso: PY=/ruta/venv/bin/python bash tests/generar_evidencias.sh
# No instala crontab: ejecuta la línea de crontab.example con un entorno mínimo como el de Cron.
set -u -o pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1
PY="${PY:-python3}"
EV="$PWD/evidencias"
export PREFECT_HOME="$(mktemp -d)" PYTHONDONTWRITEBYTECODE=1 NO_COLOR=1
export PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS=120  # el valor por defecto (20 s) puede no bastar
export PREFECT_SERVER_ANALYTICS_ENABLED=false DO_NOT_TRACK=1 PREFECT_LOGGING_COLORS=false
mkdir -p "$EV"

correr() {  # correr <comando...>: muestra comando, salida real y código.
  echo "\$ $*"
  "$@" 2>&1
  echo "[código de salida: $?]"
  echo
}

{
  echo "Fecha: $(date -Is)"; echo "Sistema: $(. /etc/os-release; echo "$PRETTY_NAME"), kernel $(uname -r)"
  echo "Python del sistema: $(/usr/bin/python3 --version)"; echo "Python de Prefect: $("$PY" --version)"
  echo "Prefect: $("$PY" -c 'import prefect; print(prefect.__version__)')"
  echo "Bash: $BASH_VERSION"; flock --version
} > "$EV/00_entorno.txt"

for e in normal temporal permanente; do correr /usr/bin/python3 cron_demo/demo_cron.py etl --escenario "$e"; done > "$EV/01_cron_sin_reintentos.txt"
for e in normal temporal permanente; do correr bash cron_demo/reintentos.sh "$e"; done > "$EV/02_cron_con_reintentos.txt"

{
  echo "# Línea C de crontab.example (con flock) bajo entorno mínimo; NO se instaló ninguna tarea."
  linea="$(grep -m1 '^#0 0 \* \* \* /usr/bin/flock' cron_demo/crontab.example)"
  cmd="${linea#\#0 0 \* \* \* }"
  echo "\$ $cmd"
  env -i HOME="$HOME" SHELL=/bin/bash PATH=/usr/bin:/bin PROYECTO="$PWD" PYTHON_BIN=/usr/bin/python3 /bin/bash -c "$cmd"
  echo "[código de salida: $?]"; echo "--- cron.log"; cat resultados/cron.log
  echo "--- cron.error.log"; cat resultados/cron.error.log
  echo; echo "# Solapamiento: segunda ejecución mientras la primera sigue activa"
  flock -n -E 99 resultados/etl.lock bash cron_demo/reintentos.sh temporal > /dev/null & p=$!
  sleep 1
  flock -n -E 99 resultados/etl.lock bash cron_demo/reintentos.sh temporal; echo "[segunda: código $?]"
  wait "$p"; echo "[primera: código $?]"
} > "$EV/03_crontab_y_flock.txt" 2>&1

correr "$PY" demo_prefect.py > "$EV/04_prefect_demo_original.txt"
for e in normal temporal permanente; do correr "$PY" demo_prefect_escenarios.py "$e"; done > "$EV/05_prefect_escenarios.txt"

{ echo "# CSV generados por las ejecuciones anteriores (permanente no debe aparecer)"
  for f in resultados/*.csv; do echo "--- $f"; tr -d '\r' < "$f"; done; } > "$EV/06_csv_generados.txt"

{
  export PREFECT_API_URL=http://127.0.0.1:4299/api
  "$PY" -m prefect server start --host 127.0.0.1 --port 4299 > "$PREFECT_HOME/server.log" 2>&1 & srv=$!
  for _ in $(seq 60); do curl -fs "$PREFECT_API_URL/health" > /dev/null && break; sleep 1; done
  echo "\$ timeout --preserve-status -s INT 150 python programacion_recurrente.py normal"
  timeout --preserve-status -s INT 150 "$PY" programacion_recurrente.py normal 2>&1
  echo "[código de salida: $?]"
  kill "$srv"; wait "$srv"; unset PREFECT_API_URL
} > "$EV/07_prefect_recurrente.txt" 2>/dev/null

correr "$PY" -W ignore -m unittest discover -s tests -v > "$EV/08_pruebas_linux.txt"
rm -rf "$PREFECT_HOME"
# Rutas y datos locales fuera de las evidencias.
sed -i -e "s#$PWD#<proyecto>#g" -e "s#$(dirname "$(dirname "$PY")")#<venv>#g" -e "s#$HOME#<home>#g" -e 's/\x1b\[[0-9;]*m//g' "$EV"/0[0-8]*.txt
