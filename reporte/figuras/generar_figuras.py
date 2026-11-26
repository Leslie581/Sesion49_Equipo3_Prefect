#!/usr/bin/env python3
"""Genera las páginas HTML de las figuras no capturadas de la UI (diagramas, gráfica y salidas
reales de terminal). Cada HTML se convierte a PNG con Edge/Chrome sin interfaz:
  python reporte/figuras/generar_figuras.py   ->  reporte/figuras/html/*.html
Los textos de terminal se copian SIN MODIFICAR de evidencias/ y del CSV de resultados/.
"""
import csv
import html
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent / "html"
OUT.mkdir(exist_ok=True)
CSS = """body{margin:0;font-family:'Segoe UI',Arial,sans-serif;background:#fff;color:#1a1a1a}
.term{background:#1e1e1e;color:#e6e6e6;font:13px/1.35 Consolas,monospace;padding:14px 18px;white-space:pre-wrap;word-break:break-word}
.term .t{color:#9cdcfe}.ok{color:#89d185}.err{color:#f48771}.cab{background:#333;color:#ccc;font:12px 'Segoe UI';padding:6px 12px}"""


def pagina(nombre, cuerpo, ancho=1100):
    (OUT / f"{nombre}.html").write_text(
        f"<!doctype html><meta charset='utf-8'><style>{CSS}</style>"
        f"<div style='width:{ancho}px'>{cuerpo}</div>", encoding="utf-8")


def terminal(nombre, titulo, texto, ancho=1100):
    t = html.escape(texto)
    for p, c in (("COMPLETED", "ok"), ("Completed", "ok"), ("Completado", "ok"), ("[código de salida: 0]", "ok"),
                 ("FAILED", "err"), ("Failed", "err"), ("[ERROR]", "err"), ("[código de salida: 1]", "err"),
                 ("Retries are exhausted", "err")):
        t = t.replace(html.escape(p), f"<span class='{c}'>{html.escape(p)}</span>")
    t = "\n".join(f"<span class='t'>{l}</span>" if l.startswith("$ ") else l for l in t.split("\n"))
    pagina(nombre, f"<div class='cab'>{html.escape(titulo)}</div><div class='term'>{t}</div>", ancho)


def leer(rel):
    return (RAIZ / rel).read_text(encoding="utf-8")


def bloque(texto, desde, hasta=None):
    i = texto.index(desde)
    j = texto.index(hasta, i + 1) if hasta else len(texto)
    return texto[i:j].rstrip()


# --- Diagrama de arquitectura -------------------------------------------------------------
def caja(x, y, w, h, titulo, sub="", color="#1f3a5f", fondo="#eef3f9"):
    s = f"<rect x='{x}' y='{y}' width='{w}' height='{h}' rx='8' fill='{fondo}' stroke='{color}' stroke-width='2'/>"
    s += f"<text x='{x+w/2}' y='{y+(24 if sub else h/2+5)}' text-anchor='middle' font-weight='700' font-size='15' fill='{color}'>{titulo}</text>"
    for k, linea in enumerate(sub.split("|") if sub else []):
        s += f"<text x='{x+w/2}' y='{y+45+k*17}' text-anchor='middle' font-size='12.5' fill='#333'>{linea}</text>"
    return s


def flecha(x1, y1, x2, y2, txt=""):
    s = f"<line x1='{x1}' y1='{y1}' x2='{x2}' y2='{y2}' stroke='#555' stroke-width='2' marker-end='url(#m)'/>"
    if txt:
        s += f"<text x='{(x1+x2)/2+4}' y='{(y1+y2)/2-6}' font-size='12' fill='#555'>{txt}</text>"
    return s


DEFS = "<defs><marker id='m' markerWidth='10' markerHeight='10' refX='9' refY='5' orient='auto'><path d='M0,0 L10,5 L0,10 z' fill='#555'/></marker></defs>"
svg = [f"<svg xmlns='http://www.w3.org/2000/svg' width='1100' height='560' font-family='Segoe UI,Arial'>{DEFS}"]
svg.append(caja(20, 20, 500, 130, "Orquestación con Cron (WSL)", "crontab: 0 12 * * *  (zona horaria del sistema)|flock -n -E 99 clima.lock  (evita solapamientos)|cron_clima.sh: bucle de reintentos 1 + 3, espera 5 s|Registro: clima_cron.log · journalctl -u cron", "#7a4b00", "#fff6e5"))
svg.append(caja(580, 20, 500, 130, "Orquestación con Prefect 3.9.5", "serve + Cron('0 12 * * *', timezone=America/Mexico_City)|@flow etl-clima-colima · @task extraer/transformar/guardar|retries=3, retry_delay_seconds=5, retry_condition_fn|Monitoreo: Prefect UI y API (http://127.0.0.1:4200)", "#0b5d3b", "#e9f7ef"))
svg.append(caja(300, 215, 500, 60, "clima_demo/etl_clima.py  (código ETL compartido)", "", "#1f3a5f"))
svg.append(flecha(270, 150, 420, 213, "invoca"))
svg.append(flecha(830, 150, 680, 213, "importa"))
svg.append(caja(20, 330, 230, 110, "Open-Meteo API", "/v1/forecast · current=|4 variables · 3 coordenadas|timezone=America/Mexico_City"))
svg.append(caja(300, 330, 230, 110, "1. Extraer", "1 petición HTTP (urllib)|errores transitorios vs|permanentes"))
svg.append(caja(580, 330, 230, 110, "2. Transformar", "validación de tipos y rangos|redondeo a 1 decimal|descarte por ciudad"))
svg.append(caja(860, 330, 220, 110, "3. Guardar", "CSV histórico|clave ciudad + fecha_hora|escritura atómica"))
svg.append(flecha(250, 385, 298, 385))
svg.append(flecha(530, 385, 578, 385))
svg.append(flecha(810, 385, 858, 385))
svg.append(flecha(550, 275, 550, 328))
svg.append(caja(300, 475, 780, 65, "resultados/clima_historico.csv", "fecha_hora_local · zona_horaria · ciudad · lat/lon · 4 variables + unidad · fuente · consultado_utc", "#444", "#f4f4f4"))
svg.append(flecha(970, 440, 970, 473))
svg.append("<text x='20' y='470' font-size='12' fill='#a00'>Fallos simulados (temporal/permanente)</text><text x='20' y='487' font-size='12' fill='#a00'>se inyectan ANTES de la petición real</text>")
svg.append("</svg>")
pagina("fig_arquitectura", "".join(svg))

# --- Diagrama de secuencia de reintentos ------------------------------------------------
def fila(y, etiqueta, celdas, res, color):
    s = f"<text x='10' y='{y+24}' font-size='14' font-weight='700'>{etiqueta}</text>"
    for k, (txt, ok) in enumerate(celdas):
        x = 230 + k * 160
        s += f"<rect x='{x}' y='{y}' width='130' height='38' rx='6' fill='{'#e9f7ef' if ok else '#fdecea'}' stroke='{'#0b5d3b' if ok else '#a00'}' stroke-width='2'/>"
        s += f"<text x='{x+65}' y='{y+24}' text-anchor='middle' font-size='12.5'>{txt}</text>"
        if k < len(celdas) - 1:
            s += f"<line x1='{x+130}' y1='{y+19}' x2='{x+158}' y2='{y+19}' stroke='#555' stroke-width='2' marker-end='url(#m)'/><text x='{x+144}' y='{y+12}' text-anchor='middle' font-size='10' fill='#555'>5 s</text>"
    s += f"<text x='{230+len(celdas)*160}' y='{y+24}' font-size='13.5' font-weight='700' fill='{color}'>{res}</text>"
    return s


svg = [f"<svg xmlns='http://www.w3.org/2000/svg' width='1100' height='250' font-family='Segoe UI,Arial'>{DEFS}"]
svg.append("<text x='230' y='22' font-size='13' fill='#555'>Intento 1</text><text x='390' y='22' font-size='13' fill='#555'>Intento 2</text><text x='550' y='22' font-size='13' fill='#555'>Intento 3</text><text x='710' y='22' font-size='13' fill='#555'>Intento 4</text>")
svg.append(fila(40, "normal", [("API real ✓", True)], "Completed / código 0", "#0b5d3b"))
svg.append(fila(100, "temporal", [("simulado ✗", False), ("simulado ✗", False), ("API real ✓", True)], "Completed / código 0", "#0b5d3b"))
svg.append(fila(160, "permanente", [("simulado ✗", False)] * 4, "Failed / código 1 (sin escritura)", "#a00"))
svg.append("<text x='10' y='236' font-size='12' fill='#555'>Misma política en ambos orquestadores: 1 intento inicial + 3 reintentos, espera de 5 s. ✗ = ConnectionError simulado; ✓ = consulta real a Open-Meteo.</text></svg>")
pagina("fig_secuencia_reintentos", "".join(svg))

# --- Gráfica de datos reales ------------------------------------------------------------
filas = list(csv.DictReader(open(RAIZ / "resultados" / "clima_historico.csv", encoding="utf-8")))
if not filas:
    sys.exit("CSV vacío")
ultima = max(f["fecha_hora_local"] for f in filas)
sel = [f for f in filas if f["fecha_hora_local"] == ultima]
vars_ = [("temperatura", "Temperatura", "°C", 40), ("humedad", "Humedad relativa", "%", 100),
         ("precipitacion", "Precipitación", "mm", 5), ("viento", "Viento a 10 m", "km/h", 20)]
colores = ["#1f3a5f", "#2f7d4f", "#b26b00"]
svg = ["<svg xmlns='http://www.w3.org/2000/svg' width='1100' height='360' font-family='Segoe UI,Arial'>"]
svg.append(f"<text x='550' y='24' text-anchor='middle' font-size='16' font-weight='700'>Condiciones actuales reportadas por Open-Meteo · {ultima} ({sel[0]['zona_horaria']})</text>")
for k, (col, nom, uni, tope) in enumerate(vars_):
    x0, y0, h = 40 + k * 265, 300, 220
    tope = max(tope, max(float(f[col]) for f in sel) * 1.2)
    svg.append(f"<text x='{x0+110}' y='60' text-anchor='middle' font-size='14' font-weight='700'>{nom} ({uni})</text>")
    svg.append(f"<line x1='{x0}' y1='{y0}' x2='{x0+220}' y2='{y0}' stroke='#888'/>")
    for j, f in enumerate(sel):
        v = float(f[col]); bh = v / tope * h; x = x0 + 15 + j * 68
        svg.append(f"<rect x='{x}' y='{y0-bh}' width='52' height='{bh}' fill='{colores[j]}'/>")
        svg.append(f"<text x='{x+26}' y='{y0-bh-6}' text-anchor='middle' font-size='13'>{v:g}</text>")
        svg.append(f"<text x='{x+26}' y='{y0+18}' text-anchor='middle' font-size='11'>{f['ciudad'].replace('Villa de Álvarez','V. de Álvarez')}</text>")
svg.append("<text x='550' y='350' text-anchor='middle' font-size='11.5' fill='#555'>Fuente: resultados/clima_historico.csv (datos reales). Escalas independientes por variable.</text></svg>")
pagina("fig_grafica_clima", "".join(svg))

# --- Salidas reales de terminal --------------------------------------------------------
ev16 = leer("evidencias/16_crontab_wsl_real.txt")
terminal("fig_crontab", "WSL · Ubuntu 24.04 — evidencias/16_crontab_wsl_real.txt (salida real)", ev16)
ev12 = leer("evidencias/12_clima_cron_escenarios.txt")
terminal("fig_cron_escenarios", "WSL — evidencias/12_clima_cron_escenarios.txt: temporal y permanente con cron_clima.sh",
         bloque(ev12, "$ bash clima_demo/cron_clima.sh temporal"))
with open(RAIZ / "resultados" / "clima_historico.csv", encoding="utf-8") as f:
    terminal("fig_csv", "resultados/clima_historico.csv (contenido real; columnas fuente y consultado_utc incluidas)",
             "$ cat resultados/clima_historico.csv\n" + f.read())
terminal("fig_csv_verificacion", "WSL — evidencias/14_clima_csv_historico.txt (verificación de duplicados)",
         bloque(leer("evidencias/14_clima_csv_historico.txt"), "# Verificación"))
terminal("fig_pruebas", "WSL — evidencias/15_clima_pruebas_linux.txt (pruebas automatizadas)", leer("evidencias/15_clima_pruebas_linux.txt"))
terminal("fig_pruebas_base", "WSL — evidencias/08_pruebas_linux.txt (demostración base, extracto)",
         "\n".join(l for l in leer("evidencias/08_pruebas_linux.txt").splitlines() if " ... " in l or l.startswith(("Ran", "OK", "$", "FAILED"))))
print("Figuras HTML en", OUT)
