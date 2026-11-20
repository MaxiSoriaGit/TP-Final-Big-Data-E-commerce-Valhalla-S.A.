-- ETAPA 6 - UNIR en tabla_final (sin duplicados, tipos correctos)
-- validos + rechazados_limpios. Si un id_pedido aparece en ambos lados (un duplicado
-- que habia sido rechazado), gana el registro valido original (prioridad 1).
CREATE OR REPLACE TABLE tabla_final AS
SELECT * EXCLUDE (prioridad)
FROM (
    SELECT id_pedido, fecha_pedido, id_cliente, email_cliente, pais, ciudad, categoria,
           producto, cantidad, precio_unitario, descuento_pct, total, estado, medio_pago,
           'valido' AS origen, 1 AS prioridad
    FROM validos
    UNION ALL
    SELECT id_pedido, fecha_pedido, id_cliente, email_cliente, pais, ciudad, categoria,
           producto, cantidad, precio_unitario, descuento_pct, total, estado, medio_pago,
           'recuperado' AS origen, 2 AS prioridad
    FROM rechazados_limpios
)
QUALIFY row_number() OVER (PARTITION BY id_pedido ORDER BY prioridad, fecha_pedido) = 1
ORDER BY id_pedido;
