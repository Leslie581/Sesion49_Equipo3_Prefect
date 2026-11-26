#!/usr/bin/env bash
# ETL de clima para Cron con reintentos manuales: 1 intento inicial + hasta 3 reintentos.
# Uso: cron_clima.sh [normal|temporal|permanente]
# Variables opcionales: PYTHON_BIN, MAX_REINTENTOS, ESPERA, CSV_CLIMA, LOG_CLIMA
# Salida: 0 éxito; 1 reintentos agotados; 2 error no reintentable o uso incorrecto.
set -u -o pipefail

ESCENARIO="${1:-normal}"
case "$ESCENARIO" in
  normal|temporal|permanente) ;;
  *) echo "Uso: $0 [normal|temporal|permanente]" >&2; exit 2 ;;
esac

AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" || exit 1
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"
MAX_REINTENTOS="${MAX_REINTENTOS:-3}"
ESPERA="${ESPERA:-5}"
CSV_CLIMA="${CSV_CLIMA:-$AQUI/../resultados/clima_historico.csv}"
LOG_CLIMA="${LOG_CLIMA:-$AQUI/../resultados/clima_cron.log}"
mkdir -p "$(dirname "$CSV_CLIMA")" "$(dirname "$LOG_CLIMA")" || exit 1

log() { printf '%s [%s] %s\n' "$(date '+%F %T %Z')" "$1" "$2" | tee -a "$LOG_CLIMA"; }

log INFO "Inicio escenario=$ESCENARIO pid=$$"
max=$((MAX_REINTENTOS + 1))
n=1
while true; do
  "$PYTHON_BIN" "$AQUI/etl_clima.py" --escenario "$ESCENARIO" --intento "$n" --salida "$CSV_CLIMA" 2>&1 | tee -a "$LOG_CLIMA"
  codigo="${PIPESTATUS[0]}"
  if [ "$codigo" -eq 0 ]; then log INFO "Completado en $n intento(s)"; exit 0; fi
  log ERROR "Intento $n/$max falló con código $codigo"
  if [ "$codigo" -ne 75 ]; then log ERROR "Error no reintentable"; exit 2; fi
  if [ "$n" -ge "$max" ]; then log ERROR "Reintentos agotados: no se guardaron datos"; exit 1; fi
  log INFO "Espera de ${ESPERA}s antes del siguiente intento"
  sleep "$ESPERA"
  n=$((n + 1))
done
