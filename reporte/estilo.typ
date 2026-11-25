#set table(stroke: 0.4pt + rgb("#b8c4d0"), inset: (x: 5pt, y: 3.2pt),
  fill: (_, y) => if y == 0 { rgb("#e3ecf5") })
#show table.cell: set text(size: 8.8pt)
#show table.cell: set par(justify: false)
#show table.cell.where(y: 0): set text(weight: "bold")
#show figure: set block(breakable: false)
#show heading.where(level: 2): it => block(above: 0.95em, below: 0.55em,
  text(size: 12pt, fill: rgb("#1f4e79"), it.body))
#show raw.where(block: true): it => block(width: 100%, fill: rgb("#f4f6f8"),
  inset: 6pt, radius: 3pt, breakable: false, text(size: 8.6pt, it))
#show link: set text(fill: rgb("#1f4e79"))
