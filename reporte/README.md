# Reporte técnico

| Archivo | Contenido |
|---|---|
| `Sesion_49_Equipo_3.pdf` / `.docx` | Reporte técnico completo (genérico, sin nombres individuales) |
| `REPORTE_SESION49.md` | Fuente única de ambos formatos |
| `estilo_informe.typ` | Estilo del PDF: encabezado, numeración de páginas, texto justificado, tablas y pies |
| `referencia.docx` | Plantilla de estilos del DOCX: Segoe UI, justificado, encabezado y número de página |
| `filtro.lua` | Usa la portada nativa de Typst en el PDF y la portada en Markdown en el DOCX |
| `figuras/` | Figuras del reporte. `ui_*.png` son capturas reales de la Prefect UI; `fig_*.png` son diagramas, una gráfica y salidas reales de terminal |
| `Sesion_49_Apellido_Nombre.pdf`, `estilo.typ` | Versión breve anterior (se conserva) |

## Regenerar

Requiere [Pandoc](https://pandoc.org/) 3 y [Typst](https://typst.app/). Desde esta carpeta:

```bash
pandoc REPORTE_SESION49.md -o Sesion_49_Equipo_3.pdf  --pdf-engine=typst -H estilo_informe.typ --lua-filter=filtro.lua
pandoc REPORTE_SESION49.md -o Sesion_49_Equipo_3.docx --reference-doc=referencia.docx --lua-filter=filtro.lua
```

En el DOCX, el índice es un campo de Word: clic derecho → *Actualizar campo* (la versión del repositorio ya está actualizada).

## Figuras

1. `python figuras/generar_figuras.py` crea `figuras/html/` con los diagramas, la gráfica (a partir de
   `resultados/clima_historico.csv`) y las salidas de `evidencias/` sin modificar.
2. `node figuras/capturar.mjs <msedge.exe|chrome.exe> <lista.json>` captura cada URL con el navegador
   sin interfaz (DevTools Protocol, Node ≥ 22, sin dependencias). `figuras/ui.json` es la lista que se
   usó con la Prefect UI (`prefect server start` en `http://127.0.0.1:4200`). Los identificadores
   corresponden a las ejecuciones de `evidencias/18_prefect_ui_escenarios.txt`.

La captura `ui_06_tarea_permanente.png` se recortó antes de la traza de error para no publicar rutas locales.
