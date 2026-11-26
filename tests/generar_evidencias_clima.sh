#!/usr/bin/env bash
# Regenera las evidencias 12 a 15 del caso Open-Meteo con ejecuciones REALES (Linux o WSL).
# Uso: PY=/ruta/venv/bin/python bash tests/generar_evidencias_clima.sh
# Usa un CSV histórico propio (resultados/evidencia_clima.csv) que se reinicia al empezar.
set -u -o pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1
PY="${PY:-python3}"
EV="$PWD/evidencias"
export PREFECT_HOME="$(mktemp -d)" PYTHONDONTWRITEBYTECODE=1 NO_COLOR=1
export PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS=120 PREFECT_LOGGING_COLORS=false
export PREFECT_SERVER_ANALYTICS_ENABLED=false DO_NOT_TRACK=1
export CSV_CLIMA="$PWD/resultados/evidencia_clima.csv" LOG_CLIMA="$PWD/resultados/evidencia_clima_cron.log"
rm -f "$CSV_CLIMA" "$LOG_CLIMA"; mkdir -p resultados

correr() { echo "\$ $*"; "$@" 2>&1; echo "[código de salida: $?]"; echo; }
anonimizar() { sed -e "s#$PWD#<proyecto>#g" -e "s#$(dirname "$(dirname "$PY")")#<venv>#g" -e "s#$HOME#<home>#g" "$1" > "$1.tmp" && mv "$1.tmp" "$1"; }

{ echo "Fecha: $(date -Is) (TZ del sistema: $(cat /etc/timezone 2>/dev/null))"
  for e in normal temporal permanente; do correr bash clima_demo/cron_clima.sh "$e"; done
} > "$EV/12_clima_cron_escenarios.txt"

{ echo "Fecha: $(date -Is)"
  for e in normal temporal permanente; do correr "$PY" clima_demo/flujo_clima_prefect.py --escenario "$e" --salida "$CSV_CLIMA"; done
} > "$EV/13_clima_prefect_escenarios.txt"

{ echo "# CSV histórico tras las 6 ejecuciones anteriores (Cron y Prefect escriben el mismo archivo)"
  echo "\$ cat resultados/evidencia_clima.csv"; cat "$CSV_CLIMA"; echo
  echo "# Verificación: filas, ciudades y duplicados por (ciudad, fecha_hora_local)"
  "$PY" - "$CSV_CLIMA" <<'EOF'
import csv, sys, collections
filas = list(csv.DictReader(open(sys.argv[1], encoding="utf-8")))
claves = collections.Counter((f["ciudad"], f["fecha_hora_local"]) for f in filas)
print("filas:", len(filas), "| ciudades:", sorted({f["ciudad"] for f in filas}))
print("duplicados:", sum(n - 1 for n in claves.values()))
print("unidades:", sorted({(f["temperatura_unidad"], f["humedad_unidad"], f["precipitacion_unidad"], f["viento_unidad"]) for f in filas}))
EOF
} > "$EV/14_clima_csv_historico.txt"

correr "$PY" -W ignore -m unittest tests.test_clima -v > "$EV/15_clima_pruebas_linux.txt"

for f in "$EV"/1[2-5]_clima_*.txt; do anonimizar "$f"; done
echo "Evidencias 12-15 generadas en $EV"
