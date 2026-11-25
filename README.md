# Sesion49_Equipo3_Prefect

Orquestación de flujos de datos con **Prefect** — Equipo 3, Ingeniería de Datos (7mo semestre, Unidad III).

## Descripción
`demo_prefect.py` implementa un flujo ETL (extraer → transformar → guardar). La extracción simula una desconexión temporal en los dos primeros intentos y se recupera gracias a `@task(retries=3, retry_delay_seconds=2)`. No usa internet ni APIs externas.

## Contenido
- `demo_prefect.py`: demostración ejecutable.
- `Equipo3_Orquestacion_Prefect.pptx` / `.pdf`: presentación.
- `Equipo_3_Actividades_Prefect.docx`: actividades.
- `Guion_Exposicion.md`: guion de la exposición.

## Requisitos
- Python 3.9 o superior
- Prefect (`pip install -U prefect`)

## Instalación
```powershell
python -m venv venv
venv\Scripts\Activate.ps1
pip install -U prefect
```

## Ejecución
```powershell
python demo_prefect.py
```
