// Estilo del reporte técnico (pandoc -> typst). Las figuras y tablas llevan numeración manual
// en su pie ("Figura N." / "Tabla N.") para que coincida con la versión DOCX.
#set page(
  header: context {
    if here().page() > 1 {
      set text(size: 8pt, fill: rgb("#5a6b7d"))
      grid(columns: (1fr, auto),
        [TecNM — Instituto Tecnológico de Colima · Ingeniería de Datos],
        [Sesión 49 · Equipo 3])
      v(-0.6em)
      line(length: 100%, stroke: 0.4pt + rgb("#b8c4d0"))
    }
  },
  footer: context {
    if here().page() > 1 {
      set align(center)
      set text(size: 8.5pt, fill: rgb("#5a6b7d"))
      counter(page).display("1")
    }
  },
)
#set par(justify: true, leading: 0.68em, spacing: 0.95em)
#show heading.where(level: 1): it => {
  block(above: 1.5em, below: 0.7em, width: 100%, stroke: (bottom: 0.8pt + rgb("#1f3a5f")), inset: (bottom: 3pt),
    text(size: 14pt, weight: "bold", fill: rgb("#1f3a5f"), it.body))
}
#show heading.where(level: 2): it => block(above: 1.1em, below: 0.55em,
  text(size: 11.5pt, weight: "bold", fill: rgb("#1f4e79"), it.body))
#show outline.entry.where(level: 1): set text(weight: "bold")
#show figure.caption: it => block(width: 92%, text(size: 8.8pt, fill: rgb("#333333"), it.body))
#show figure.where(kind: table): set figure.caption(position: top)
#show figure: set block(breakable: false, above: 1em, below: 1.1em)
#set table(stroke: 0.4pt + rgb("#b8c4d0"), inset: (x: 5pt, y: 3.5pt),
  fill: (_, y) => if y == 0 { rgb("#e3ecf5") })
#show table.cell: set text(size: 8.6pt)
#show table.cell: set par(justify: false)
#show table.cell.where(y: 0): set text(weight: "bold")
#show raw.where(block: true): it => block(width: 100%, fill: rgb("#f4f6f8"), stroke: 0.4pt + rgb("#d5dde5"),
  inset: 7pt, radius: 3pt, breakable: true, text(size: 8pt, it))
#show link: set text(fill: rgb("#1f4e79"))
