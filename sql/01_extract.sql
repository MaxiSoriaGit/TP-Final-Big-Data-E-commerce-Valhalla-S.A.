-- ETAPA 1 - EXTRAER
-- Lee el CSV original (que NUNCA se modifica) y lo deja en una zona de
-- staging (parquet). Todo se lee como texto (all_varchar) para no perder
-- nada: la tipificacion se hace recien en la etapa de validacion.
COPY (
    SELECT *
    FROM read_csv('$CSV', all_varchar = true, header = true, null_padding = true)
) TO '$STAGING' (FORMAT parquet);
