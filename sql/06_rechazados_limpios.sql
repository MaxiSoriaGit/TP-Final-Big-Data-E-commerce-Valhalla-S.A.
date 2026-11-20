-- ETAPA 5 (paso 2) - Re-validar lo limpiado (03_validar.sql sobre limpieza_cruda)
-- rechazados_limpios: rechazados que ahora SI cumplen las 15 reglas, con tipos correctos.
-- descartados: lo que no se pudo recuperar (o es un id repetido dentro de los rechazados).
CREATE OR REPLACE TABLE rechazados_limpios AS
SELECT
    trim(id_pedido)                                              AS id_pedido,
    CAST(trim(fecha_pedido) AS DATE)                             AS fecha_pedido,
    trim(id_cliente)                                             AS id_cliente,
    trim(email_cliente)                                          AS email_cliente,
    trim(pais)                                                   AS pais,
    trim(ciudad)                                                 AS ciudad,
    trim(categoria)                                              AS categoria,
    trim(producto)                                               AS producto,
    CAST(trim(cantidad) AS INTEGER)                              AS cantidad,
    CAST(CAST(trim(precio_unitario) AS DOUBLE) AS DECIMAL(12,2)) AS precio_unitario,
    CAST(CAST(trim(descuento_pct) AS DOUBLE) AS DECIMAL(5,2))    AS descuento_pct,
    CAST(CAST(trim(total) AS DOUBLE) AS DECIMAL(14,2))           AS total,
    trim(estado)                                                 AS estado,
    trim(medio_pago)                                             AS medio_pago,
    motivo_original                                              AS motivo_rechazo_original
FROM limpieza_evaluada
WHERE motivo_rechazo IS NULL
ORDER BY _fila;

CREATE OR REPLACE TABLE descartados AS
SELECT
    _fila, id_pedido, fecha_pedido, id_cliente, email_cliente, pais, ciudad, categoria,
    producto, cantidad, precio_unitario, descuento_pct, total, estado, medio_pago,
    motivo_original AS motivo_rechazo_original,
    motivo_rechazo  AS motivo_tras_limpieza
FROM limpieza_evaluada
WHERE motivo_rechazo IS NOT NULL
ORDER BY _fila;

DROP TABLE limpieza_cruda;
DROP TABLE limpieza_evaluada;
