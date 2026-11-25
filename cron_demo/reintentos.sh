#!/usr/bin/env bash
# Reintentos manuales para Cron: 1 intento inicial + hasta 3 reintentos.
#
# Uso: reintentos.sh [normal|temporal|permanente]
# Variables opcionales: PYTHON_BIN, MAX_REINTENTOS, ESPERA, DIR_RESULTADOS
# Salida: 0 éxito; 1 reintentos agotados; 2 uso incorrecto;
#         otro valor: falló una etapa posterior a la extracción.
set -u -o pipefail

ESCENARIO="${1:-normal}"
case "$ESCENARIO" in
  normal|temporal|permanente) ;;
  *) echo "Uso: $0 [normal|temporal|permanente]" >&2; exit 2 ;;
esac

AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" || exit 1
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"
MAX_REINTENTOS="${MAX_REINTENTOS:-3}"
ESPERA="${ESPERA:-2}"
DIR_RESULTADOS="${DIR_RESULTADOS:-$AQUI/../resultados}"

mkdir -p "$DIR_RESULTADOS" || exit 1
ID="$(date +%Y%m%d_%H%M%S)_$$"
LOG="$DIR_RESULTADOS/cron_${ESCENARIO}_${ID}.log"
CSV="$DIR_RESULTADOS/cron_${ESCENARIO}_${ID}.csv"

# Estado aislado por ejecución; se borra al terminar, también si hay error o señal.
ESTADO="$(mktemp -d "${TMPDIR:-/tmp}/sesion49.XXXXXX")" || exit 1
trap 'rm -rf -- "$ESTADO"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

log() { printf '%s [%s] %s\n' "$(date '+%F %T')" "$1" "$2" | tee -a "$LOG"; }
etapa() {  # Ejecuta una etapa, registra su salida y devuelve SU código (no el de tee).
  "$PYTHON_BIN" "$AQUI/demo_cron.py" "$@" 2>&1 | tee -a "$LOG"
  return "${PIPESTATUS[0]}"
}

log INFO "Inicio escenario=$ESCENARIO id=$ID"
max=$((MAX_REINTENTOS + 1))
n=1
while true; do
  etapa extraer --escenario "$ESCENARIO" --estado "$ESTADO/intentos" --salida "$ESTADO/crudo.json"
  codigo=$?
  [ "$codigo" -eq 0 ] && break
  log ERROR "Intento $n/$max falló con código $codigo"
  if [ "$codigo" -eq 2 ]; then
    log ERROR "Error de configuración: no tiene sentido reintentar"
    exit 2
  fi
  if [ "$n" -ge "$max" ]; then
    log ERROR "Reintentos agotados: no se ejecutan transformar ni guardar"
    exit 1
  fi
  log INFO "Espera de ${ESPERA}s antes del siguiente intento"
  sleep "$ESPERA"
  n=$((n + 1))
done

etapa transformar --entrada "$ESTADO/crudo.json" --salida "$ESTADO/limpio.json"
codigo=$?
[ "$codigo" -eq 0 ] || { log ERROR "transformar falló con código $codigo"; exit "$codigo"; }
etapa guardar --entrada "$ESTADO/limpio.json" --salida "$CSV"
codigo=$?
[ "$codigo" -eq 0 ] || { log ERROR "guardar falló con código $codigo"; exit "$codigo"; }
log INFO "Completado en $n intento(s): $(basename "$CSV")"
