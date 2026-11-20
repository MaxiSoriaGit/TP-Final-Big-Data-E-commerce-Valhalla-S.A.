# Pipeline ETL - Valhalla S.A. (pedidos online)

## Estructura
```
valhalla/
├── datos/pedidos.csv            # origen (NO se modifica)
├── pipeline.py                  # funciones del ETL (se puede correr solo)
├── sql/                         # carpeta de consultas SQL
│   ├── 01_extract.sql ... 07_tabla_final.sql   # una por etapa
│   └── consultas.sql            # las 5 preguntas de negocio
├── dags/valhalla_pedidos_dag.py # DAG de Airflow (0 2 * * *)
├── valhalla.duckdb              # base resultante
└── informe.md                   # informe (lo genera el pipeline)
```

## Correr solo el pipeline (sin Airflow)
```bash
pip install duckdb
python pipeline.py
```
Genera/actualiza `valhalla.duckdb` e `informe.md`.

## Correr con Airflow 3.0 (Windows: dentro de WSL, como en la guia de clase)
```bash
uv venv env --python 3.12 && source env/bin/activate
uv pip install "apache-airflow[celery]==3.0.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.10.txt"
uv pip install duckdb
export AIRFLOW__CORE__DAGS_FOLDER="/ruta/a/valhalla/dags"
airflow standalone          # UI en http://localhost:8080 (usuario admin)
```
Despues, en la UI: DAG `valhalla_pedidos` -> activarlo (queda programado a las 2:00 AM)
o **Trigger** para correrlo ya y ver las 8 tareas en verde.

## Tablas en valhalla.duckdb
`raw_pedidos` -> `pedidos_evaluados` -> `validos` / `rechazados` -> `rechazados_limpios` / `descartados` -> `tabla_final`
