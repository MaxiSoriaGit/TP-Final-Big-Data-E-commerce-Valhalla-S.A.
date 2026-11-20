-- ETAPA 3 - VALIDAR (15 reglas con nombre)
-- Se usa dos veces: sobre raw_pedidos y, despues de limpiar, sobre los rechazados.
-- Placeholders que reemplaza pipeline.py: $ORIGEN, $DESTINO, $EXTRA.
-- Cada regla es un CASE que devuelve su NOMBRE cuando el pedido la INCUMPLE;
-- motivo_rechazo junta los nombres de todas las reglas incumplidas ('; ').
-- IMPORTANTE: cada regla va en UNA linea y termina con un comentario "-- descripcion"
-- (pipeline.py lo lee para armar el informe).
CREATE OR REPLACE TABLE $DESTINO AS
WITH base AS (
    SELECT
        rowid AS _fila,
        id_pedido, fecha_pedido, id_cliente, email_cliente, pais, ciudad, categoria,
        producto, cantidad, precio_unitario, descuento_pct, total, estado, medio_pago
        $EXTRA,
        nullif(trim(id_pedido), '')                  AS id_t,
        try_cast(trim(fecha_pedido) AS DATE)         AS fecha_t,
        try_cast(trim(cantidad) AS INTEGER)          AS cantidad_t,
        try_cast(trim(precio_unitario) AS DOUBLE)    AS precio_t,
        try_cast(trim(descuento_pct) AS DOUBLE)      AS descuento_t,
        try_cast(trim(total) AS DOUBLE)              AS total_t,
        row_number() OVER (PARTITION BY nullif(trim(id_pedido), '') ORDER BY rowid) AS orden_id
    FROM $ORIGEN
)
SELECT
    _fila,
    id_pedido, fecha_pedido, id_cliente, email_cliente, pais, ciudad, categoria,
    producto, cantidad, precio_unitario, descuento_pct, total, estado, medio_pago
    $EXTRA,
    nullif(concat_ws('; ',
        CASE WHEN id_t IS NULL THEN 'id_pedido_no_nulo' END,  -- id_pedido obligatorio
        CASE WHEN id_t IS NOT NULL AND orden_id > 1 THEN 'id_pedido_unico' END,  -- id_pedido no repetido (se conserva la primera aparicion)
        CASE WHEN fecha_t IS NULL OR NOT regexp_matches(trim(fecha_pedido), '^\d{4}-\d{2}-\d{2}$') OR fecha_t < DATE '2000-01-01' OR fecha_t > current_date THEN 'fecha_valida' END,  -- fecha en formato YYYY-MM-DD, real y no futura
        CASE WHEN nullif(trim(id_cliente), '') IS NULL THEN 'id_cliente_no_nulo' END,  -- id_cliente obligatorio
        CASE WHEN NOT coalesce(regexp_matches(trim(email_cliente), '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'), false) THEN 'email_valido' END,  -- email con formato usuario@dominio.ext
        CASE WHEN nullif(trim(pais), '') IS NULL THEN 'pais_no_nulo' END,  -- pais de entrega obligatorio
        CASE WHEN nullif(trim(ciudad), '') IS NULL THEN 'ciudad_no_nula' END,  -- ciudad de entrega obligatoria
        CASE WHEN nullif(trim(categoria), '') IS NULL THEN 'categoria_no_nula' END,  -- categoria del producto obligatoria
        CASE WHEN nullif(trim(producto), '') IS NULL THEN 'producto_no_nulo' END,  -- producto obligatorio
        CASE WHEN cantidad_t IS NULL OR cantidad_t <= 0 THEN 'cantidad_positiva' END,  -- cantidad entera mayor a 0
        CASE WHEN precio_t IS NULL OR precio_t <= 0 THEN 'precio_numerico_positivo' END,  -- precio_unitario numerico (punto decimal) mayor a 0
        CASE WHEN descuento_t IS NULL OR descuento_t < 0 OR descuento_t > 100 THEN 'descuento_en_rango' END,  -- descuento_pct entre 0 y 100
        CASE WHEN total_t IS NULL OR abs(total_t - cantidad_t * precio_t * (1 - descuento_t / 100.0)) > 0.05 THEN 'total_coherente' END,  -- total presente y igual a cantidad*precio*(1-descuento/100)
        CASE WHEN estado IS NULL OR estado NOT IN ('entregado', 'enviado', 'pendiente', 'cancelado') THEN 'estado_valido' END,  -- estado exacto: entregado/enviado/pendiente/cancelado
        CASE WHEN medio_pago IS NULL OR medio_pago NOT IN ('tarjeta', 'transferencia', 'efectivo', 'billetera_virtual') THEN 'medio_pago_valido' END  -- medio_pago exacto: tarjeta/transferencia/efectivo/billetera_virtual
    ), '') AS motivo_rechazo
FROM base
ORDER BY _fila;
