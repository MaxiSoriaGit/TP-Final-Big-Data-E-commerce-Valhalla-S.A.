"""
DAG de Airflow - Pipeline de pedidos de Valhalla S.A.

Corre todos los dias a las 02:00 (hora Argentina): cron "0 2 * * *".
Cada etapa del ETL es una tarea (@task) y se encadenan con >>:

    extraer >> cargar_raw >> validar >> separar >> limpiar >> tabla_final >> consultas >> informe

Cada tarea abre su propia conexion a DuckDB y la cierra al terminar
(DuckDB admite un solo escritor a la vez, y las tareas corren una detras de otra).
"""
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

# Para poder hacer "import pipeline" (esta en la carpeta de arriba de dags/)
RAIZ_PROYECTO = Path(__file__).resolve().parents[1]
if str(RAIZ_PROYECTO) not in sys.path:
    sys.path.insert(0, str(RAIZ_PROYECTO))

import pipeline  # noqa: E402


@dag(
    dag_id="valhalla_pedidos",
    description="ETL diario de pedidos online: validar, limpiar y consolidar en tabla_final",
    schedule="0 2 * * *",  # todos los dias a las 2:00 AM
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,  # no ejecutar los dias pasados que se perdieron
    # si una tarea falla, reintenta una vez a los 30 segundos
    # (por defecto Airflow espera 5 minutos, y parece que se colgo)
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["valhalla", "etl", "duckdb"],
)
def valhalla_pedidos():
    @task
    def extraer():
        with pipeline.conectar() as con:
            return pipeline.extract(con)

    @task
    def cargar_raw():
        with pipeline.conectar() as con:
            return pipeline.load_raw(con)

    @task
    def validar():
        with pipeline.conectar() as con:
            return pipeline.validate(con)

    @task
    def separar():
        with pipeline.conectar() as con:
            return pipeline.split(con)

    @task
    def limpiar():
        with pipeline.conectar() as con:
            return pipeline.clean(con)

    @task
    def tabla_final():
        with pipeline.conectar() as con:
            return pipeline.build_final(con)

    @task
    def consultas():
        with pipeline.conectar() as con:
            pipeline.run_queries(con)

    @task
    def informe():
        with pipeline.conectar() as con:
            pipeline.write_report(con, pipeline.run_queries(con))

    t_extraer = extraer()
    t_cargar = cargar_raw()
    t_validar = validar()
    t_separar = separar()
    t_limpiar = limpiar()
    t_final = tabla_final()
    t_consultas = consultas()
    t_informe = informe()

    t_extraer >> t_cargar >> t_validar >> t_separar >> t_limpiar >> t_final >> t_consultas >> t_informe


valhalla_pedidos()
