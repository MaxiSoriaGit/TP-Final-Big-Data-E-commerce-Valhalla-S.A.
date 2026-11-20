-- ETAPA 5 - LIMPIAR los rechazados (paso 1: corregir los valores, todavia como texto)
-- Correcciones, basadas en los problemas que aparecen en el CSV:
--   fecha         dd/mm/yyyy (o dd-mm-yyyy)       -> YYYY-MM-DD
--   precio        "210.000,00" (formato europeo)  -> 210000.00
--   cantidad      negativa (error de signo)       -> valor absoluto
--   total         vacio o inconsistente           -> se recalcula: cantidad*precio*(1-desc/100)
--   estado/pago   mayusculas y espacios sobrantes -> minusculas y trim
--   pais vacio    -> se completa desde la ciudad   (usando los pedidos validos)
--   categoria vacia -> se completa desde el producto (usando los pedidos validos)
-- Lo que despues de esto siga incumpliendo reglas va a "descartados" (ver 06).
CREATE OR REPLACE TABLE limpieza_cruda AS
WITH mapa_pais AS (
    SELECT ciudad, min(pais) AS pais
    FROM validos
    GROUP BY ciudad
    HAVING count(DISTINCT pais) = 1
),
mapa_categoria AS (
    SELECT producto, min(categoria) AS categoria
    FROM validos
    GROUP BY producto
    HAVING count(DISTINCT categoria) = 1
),
paso1 AS (
    SELECT
        r._fila,
        r.motivo_rechazo AS motivo_original,
        trim(r.id_pedido) AS id_pedido,
        strftime(
            coalesce(
                try_strptime(trim(r.fecha_pedido), '%Y-%m-%d'),
                try_strptime(trim(r.fecha_pedido), '%d/%m/%Y'),
                try_strptime(trim(r.fecha_pedido), '%d-%m-%Y')
            ), '%Y-%m-%d') AS fecha_pedido,
        trim(r.id_cliente) AS id_cliente,
        lower(trim(r.email_cliente)) AS email_cliente,
        coalesce(nullif(trim(r.pais), ''), mp.pais) AS pais,
        trim(r.ciudad) AS ciudad,
        coalesce(nullif(trim(r.categoria), ''), mc.categoria) AS categoria,
        trim(r.producto) AS producto,
        abs(try_cast(trim(r.cantidad) AS INTEGER)) AS cant,
        try_cast(
            CASE WHEN contains(r.precio_unitario, ',')
                 THEN replace(replace(trim(r.precio_unitario), '.', ''), ',', '.')
                 ELSE trim(r.precio_unitario) END
            AS DOUBLE) AS prec,
        CASE WHEN nullif(trim(r.descuento_pct), '') IS NULL THEN 0
             ELSE try_cast(trim(r.descuento_pct) AS DOUBLE) END AS dto,
        lower(trim(r.estado)) AS estado,
        lower(trim(r.medio_pago)) AS medio_pago
    FROM rechazados r
    LEFT JOIN mapa_pais mp ON trim(r.ciudad) = mp.ciudad
    LEFT JOIN mapa_categoria mc ON trim(r.producto) = mc.producto
)
SELECT
    motivo_original,
    id_pedido, fecha_pedido, id_cliente, email_cliente, pais, ciudad, categoria, producto,
    CAST(cant AS VARCHAR) AS cantidad,
    CAST(prec AS VARCHAR) AS precio_unitario,
    CAST(dto AS VARCHAR)  AS descuento_pct,
    CAST(round(cant * prec * (1 - dto / 100.0), 2) AS VARCHAR) AS total,
    estado, medio_pago
FROM paso1
ORDER BY _fila;
