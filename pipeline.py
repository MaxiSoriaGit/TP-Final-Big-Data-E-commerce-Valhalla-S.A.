"""
Pipeline ETL - Valhalla S.A. (pedidos online)

Etapas (cada una es una funcion; el DAG de Airflow las llama como tareas):
    extract          1. Extraer el CSV original (sin modificarlo) a staging
    load_raw         2. Cargarlo "tal cual" en raw_pedidos
    validate         3. Validar con 15 reglas con nombre
    split            4. Separar en validos y rechazados (con motivo_rechazo)
    clean            5. Limpiar los rechazados -> rechazados_limpios (+ descartados)
    build_final      6. Unir en tabla_final (sin duplicados, tipos correctos)
    run_queries      7. Responder las preguntas de negocio (sql/consultas.sql)
    write_report     Informe breve en informe.md

Uso directo:  python pipeline.py
Con Airflow:  ver dags/valhalla_pedidos_dag.py
"""
import re
from datetime import datetime
from pathlib import Path

import duckdb

# Rutas
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "datos"
OUTPUT_DIR = BASE_DIR / "output"
SQL_DIR = BASE_DIR / "sql"
CSV_ORIGEN = DATA_DIR / "pedidos.csv"
STAGING = OUTPUT_DIR / "pedidos_extraido.parquet"
DB_PATH = BASE_DIR / "valhalla.duckdb"
INFORME = BASE_DIR / "informe.md"

COLUMNAS = [
    "id_pedido", "fecha_pedido", "id_cliente", "email_cliente", "pais", "ciudad",
    "categoria", "producto", "cantidad", "precio_unitario", "descuento_pct",
    "total", "estado", "medio_pago",
]


# ---------------------------------------------------------------- utilidades
def read_sql(filename: str, **reemplazos) -> str:
    """Lee un archivo de sql/ y reemplaza los placeholders $CLAVE por su valor."""
    try:
        texto = (SQL_DIR / filename).read_text(encoding="utf-8")
    except Exception as e:
        print("Ocurrio un error a leer el sql", e)
        raise
    for clave, valor in reemplazos.items():
        texto = texto.replace(f"${clave}", valor)
    return texto


def sql_path(path: Path) -> str:
    """Ruta lista para ir dentro de un literal SQL ('...')."""
    return path.as_posix().replace("'", "''")


def count(con, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def conectar():
    """Abre la base valhalla.duckdb (usar con 'with' para que se cierre sola).

    Si el archivo lo creo otra version de DuckDB y esta no puede leerlo, se
    guarda como .bak y se crea uno nuevo. No se pierde nada: el pipeline
    reconstruye todas las tablas desde el CSV en cada ejecucion.
    """
    try:
        return duckdb.connect(str(DB_PATH))
    except duckdb.IOException as e:
        print("No se pudo abrir la base existente, se recrea:", e)
        DB_PATH.replace(DB_PATH.with_suffix(".duckdb.bak"))
        return duckdb.connect(str(DB_PATH))


# -------------------------------------------------------------------- etapas
def extract(con) -> int:
    print("######### 1. Extraccion #########")
    if not CSV_ORIGEN.exists():
        raise FileNotFoundError(f"No existe el origen: {CSV_ORIGEN}")
    OUTPUT_DIR.mkdir(exist_ok=True)

    esquema = con.execute(
        f"DESCRIBE SELECT * FROM read_csv('{sql_path(CSV_ORIGEN)}', "
        "all_varchar = true, header = true, null_padding = true)"
    ).fetchall()
    faltantes = set(COLUMNAS) - {fila[0] for fila in esquema}
    if faltantes:
        raise ValueError(f"Al CSV le faltan columnas: {sorted(faltantes)}")

    con.execute(read_sql("01_extract.sql", CSV=sql_path(CSV_ORIGEN), STAGING=sql_path(STAGING)))
    filas = con.execute(f"SELECT COUNT(*) FROM read_parquet('{sql_path(STAGING)}')").fetchone()[0]
    print("Filas extraidas:", filas)
    return filas


def load_raw(con) -> int:
    print("######### 2. Carga en raw_pedidos #########")
    con.execute(read_sql("02_load_raw.sql", STAGING=sql_path(STAGING)))
    filas = count(con, "raw_pedidos")
    print("Filas en raw_pedidos:", filas)
    return filas


def evaluar(con, origen: str, destino: str, extra: str = "") -> None:
    """Aplica las 15 reglas de sql/03_validar.sql sobre la tabla 'origen'."""
    con.execute(read_sql("03_validar.sql", ORIGEN=origen, DESTINO=destino, EXTRA=extra))


def validate(con) -> int:
    print("######### 3. Validacion (15 reglas) #########")
    evaluar(con, "raw_pedidos", "pedidos_evaluados")
    con_error = con.execute(
        "SELECT COUNT(*) FROM pedidos_evaluados WHERE motivo_rechazo IS NOT NULL"
    ).fetchone()[0]
    print("Pedidos que incumplen al menos una regla:", con_error)
    return con_error


def split(con) -> dict:
    print("######### 4. Separar validos / rechazados #########")
    con.execute(read_sql("04_separar.sql"))
    resultado = {"validos": count(con, "validos"), "rechazados": count(con, "rechazados")}
    print("Validos:", resultado["validos"], "| Rechazados:", resultado["rechazados"])
    return resultado


def clean(con) -> dict:
    print("######### 5. Limpieza de rechazados #########")
    con.execute(read_sql("05_limpiar.sql"))
    evaluar(con, "limpieza_cruda", "limpieza_evaluada", extra=", motivo_original")
    con.execute(read_sql("06_rechazados_limpios.sql"))
    resultado = {
        "rechazados_limpios": count(con, "rechazados_limpios"),
        "descartados": count(con, "descartados"),
    }
    print("Recuperados:", resultado["rechazados_limpios"], "| Descartados:", resultado["descartados"])
    return resultado


def build_final(con) -> int:
    print("######### 6. tabla_final #########")
    con.execute(read_sql("07_tabla_final.sql"))
    total, unicos = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT id_pedido) FROM tabla_final"
    ).fetchone()
    if total != unicos:
        raise ValueError(f"tabla_final tiene duplicados: {total} filas, {unicos} ids unicos")
    print("Filas en tabla_final:", total, "(sin duplicados)")
    return total


def run_queries(con) -> list:
    """Ejecuta sql/consultas.sql y devuelve [(titulo, columnas, filas), ...]."""
    print("######### 7. Preguntas de negocio #########")
    resultados = []
    for bloque in read_sql("consultas.sql").split(";"):
        titulo = re.search(r"--\s*(Q\d+:.*)", bloque)
        sentencia = "\n".join(
            l for l in bloque.splitlines() if not l.strip().startswith("--")
        ).strip()
        if not titulo or not sentencia:
            continue
        cursor = con.execute(sentencia)
        columnas = [d[0] for d in cursor.description]
        filas = cursor.fetchall()
        resultados.append((titulo.group(1).strip(), columnas, filas))
        print("\n" + titulo.group(1).strip())
        print(" | ".join(columnas))
        for fila in filas:
            print(" | ".join(str(v) for v in fila))
    return resultados


# ------------------------------------------------------------------- informe
def reglas() -> list:
    """Lee (nombre, descripcion) de las reglas directamente de sql/03_validar.sql."""
    texto = read_sql("03_validar.sql")
    return re.findall(r"THEN '(\w+)' END\s*,?\s*--\s*(.+)", texto)


def _num(valor) -> str:
    """Formato argentino: 1.234.567,89"""
    if isinstance(valor, int):
        return f"{valor:,}".replace(",", ".")
    texto = f"{float(valor):,.2f}"
    return texto.replace(",", "X").replace(".", ",").replace("X", ".")


def _tabla_md(columnas, filas) -> str:
    lineas = ["| " + " | ".join(columnas) + " |", "|" + "|".join("---" for _ in columnas) + "|"]
    for fila in filas:
        celdas = [_num(v) if isinstance(v, (int, float)) or hasattr(v, "quantize") else str(v) for v in fila]
        lineas.append("| " + " | ".join(celdas) + " |")
    return "\n".join(lineas)


def write_report(con, resultados: list) -> Path:
    print("######### Informe #########")
    n = lambda sql, *p: con.execute(sql, list(p)).fetchone()[0]
    raw = count(con, "raw_pedidos")
    ids_unicos = n("SELECT COUNT(DISTINCT id_pedido) FROM raw_pedidos")
    validos = count(con, "validos")
    rechazados = count(con, "rechazados")
    recuperados = count(con, "rechazados_limpios")
    descartados = count(con, "descartados")
    final = count(con, "tabla_final")
    dup_union = validos + recuperados - final

    filas_reglas = []
    for nombre, descripcion in reglas():
        fallas = n(
            "SELECT COUNT(*) FROM pedidos_evaluados "
            "WHERE list_contains(string_split(motivo_rechazo, '; '), ?)", nombre
        )
        filas_reglas.append((nombre, descripcion.strip(), fallas))

    respuestas = "\n\n".join(
        f"**{titulo}**\n\n{_tabla_md(columnas, filas)}" for titulo, columnas, filas in resultados
    )

    md = f"""# Informe de calidad de datos - Pedidos online Valhalla S.A.

*Generado automaticamente por el pipeline el {datetime.now():%d/%m/%Y %H:%M}.*

## 1. Que se hizo
Pipeline ETL con Python + DuckDB (`pipeline.py`, SQL en `sql/`) orquestado con un DAG de Airflow
(`dags/valhalla_pedidos_dag.py`, cron `0 2 * * *`, todos los dias a las 2:00 AM).
El archivo `datos/pedidos.csv` nunca se modifica: se lee, se copia a `raw_pedidos` tal cual y
todo el trabajo posterior ocurre en `valhalla.duckdb`.

## 2. Resultado de la calidad
| Etapa | Filas |
|---|---|
| Leidas del CSV (`raw_pedidos`) | {_num(raw)} ({_num(ids_unicos)} `id_pedido` distintos) |
| Validas a la primera (`validos`) | {_num(validos)} |
| Rechazadas (`rechazados`) | {_num(rechazados)} |
| Recuperadas tras limpiar (`rechazados_limpios`) | {_num(recuperados)} |
| Descartadas (`descartados`) | {_num(descartados)} |
| **Pedidos confiables (`tabla_final`)** | **{_num(final)}** |

Duplicados eliminados al unir validos con recuperados: {_num(dup_union)}.

## 3. Reglas de validacion (15) y filas que las incumplen
Una fila puede incumplir varias reglas a la vez.

| Regla | Que controla | Filas |
|---|---|---|
""" + "\n".join(f"| `{n_}` | {d} | {_num(f)} |" for n_, d, f in filas_reglas) + f"""

## 4. Limpieza aplicada a los rechazados
Fechas `dd/mm/yyyy` a ISO; precios `210.000,00` a numero; cantidades negativas a valor absoluto
(en todos los casos el total original coincidia con el valor absoluto); total recalculado como
`cantidad * precio * (1 - descuento/100)`; estado y medio de pago en minusculas y sin espacios;
pais faltante completado desde la ciudad y categoria faltante desde el producto.

## 5. Respuestas a las preguntas de negocio
{respuestas}

## 6. Supuestos
- Facturacion = suma de `total` sin pedidos cancelados (se aplica a Q1, Q2 y Q3). Q4 usa todos los pedidos.
- Fechas con barra se interpretan como dia/mes/anio.
- Una cantidad negativa se considera un error de signo.
- Los totales se recalculan a partir de cantidad, precio y descuento.
"""
    INFORME.write_text(md, encoding="utf-8")
    print("Informe escrito en", INFORME)
    return INFORME


# ---------------------------------------------------------------------- main
def main():
    with conectar() as con:
        extract(con)
        load_raw(con)
        validate(con)
        split(con)
        clean(con)
        build_final(con)
        resultados = run_queries(con)
        write_report(con, resultados)
    print("\n######## Pipeline terminado correctamente! ########")


if __name__ == "__main__":
    main()
