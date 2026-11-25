# Reporte individual

Requiere [Pandoc](https://pandoc.org/) y [Typst](https://typst.app/). Desde esta carpeta:

```bash
pandoc REPORTE_SESION49.md -o Sesion_49_Apellido_Nombre.pdf --pdf-engine=typst -H estilo.typ
```

Para la entrega, cambiar el nombre del archivo y agregar `-M author="Nombre Apellido"`.
Git ignora cualquier `Sesion_49_*.pdf` distinto de la versión sin nombre.
