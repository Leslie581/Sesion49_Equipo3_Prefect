-- La portada en Markdown (custom-style "Portada") solo se usa en DOCX; en Typst la portada es un bloque nativo.
function Div(el)
  if FORMAT:match("typst") and el.attributes["custom-style"] == "Portada" then return {} end
end
