"""
Demostración: orquestación con Prefect (Equipo 3 · Ingeniería de Datos · LCS2)

- Extracción que simula una desconexión temporal en los dos primeros intentos.
- @task(retries=3, retry_delay_seconds=2): hasta 3 reintentos, 2 s de espera.
- La extracción se recupera en el tercer intento (reintento 2).
- No usa internet ni APIs externas: el fallo se simula con un contador local.

Instalación:  pip install -U prefect
Ejecución:    python demo_prefect.py
"""

from prefect import flow, task

FALLOS_SIMULADOS = 2          # los dos primeros intentos fallan
_intentos = {"extraer": 0}    # contador local (los reintentos ocurren en el mismo proceso)


@task(retries=3, retry_delay_seconds=2, log_prints=True)
def extraer() -> list[int]:
    _intentos["extraer"] += 1
    n = _intentos["extraer"]
    print(f"Extraer: intento {n}")
    if n <= FALLOS_SIMULADOS:
        raise ConnectionError(f"Desconexión temporal simulada (intento {n})")
    datos = [10, 20, 30]
    print(f"Extraer: conexión recuperada en el intento {n} -> {datos}")
    return datos


@task(log_prints=True)
def transformar(datos: list[int]) -> list[int]:
    resultado = [x * 2 for x in datos]
    print(f"Transformar: {datos} -> {resultado}")
    return resultado


@task(log_prints=True)
def guardar(datos: list[int]) -> int:
    total = sum(datos)
    print(f"Guardar: {len(datos)} registros guardados (total = {total})")
    return total


@flow(name="pipeline-etl", log_prints=True)
def pipeline() -> int:
    datos = extraer()
    limpios = transformar(datos)
    total = guardar(limpios)
    print(f"Resultado final: {limpios} | total = {total}")
    return total


if __name__ == "__main__":
    pipeline()
