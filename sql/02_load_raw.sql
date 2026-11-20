-- ETAPA 2 - CARGAR "TAL CUAL"
-- raw_pedidos es una copia fiel del origen: todas las columnas como texto,
-- sin limpiar ni corregir nada.
CREATE OR REPLACE TABLE raw_pedidos AS
SELECT *
FROM read_parquet('$STAGING');
