-- PREGUNTAS DE NEGOCIO (todas sobre tabla_final)
-- Cada consulta empieza con un comentario "-- Qn: ..." y termina en punto y coma
-- Criterio: "facturacion" = suma de total SIN pedidos cancelados.

-- Q1: Las 3 categorias con mayor facturacion total (excluyendo cancelados)
SELECT
    categoria,
    ROUND(SUM(total), 2) AS facturacion_total
FROM tabla_final
WHERE estado <> 'cancelado'
GROUP BY categoria
ORDER BY facturacion_total DESC
LIMIT 3;

-- Q2: Facturacion por mes (excluyendo cancelados)
SELECT
    strftime(fecha_pedido, '%Y-%m') AS mes,
    COUNT(*) AS pedidos,
    ROUND(SUM(total), 2) AS facturacion
FROM tabla_final
WHERE estado <> 'cancelado'
GROUP BY mes
ORDER BY mes;

-- Q3: Ticket promedio (total promedio por pedido) por pais (excluyendo cancelados)
SELECT
    pais,
    COUNT(*) AS pedidos,
    ROUND(AVG(total), 2) AS ticket_promedio
FROM tabla_final
WHERE estado <> 'cancelado'
GROUP BY pais
ORDER BY ticket_promedio DESC;

-- Q4: Porcentaje de pedidos cancelados (sobre todos los pedidos)
SELECT
    COUNT(*) AS pedidos_totales,
    COUNT(*) FILTER (WHERE estado = 'cancelado') AS pedidos_cancelados,
    ROUND(100.0 * COUNT(*) FILTER (WHERE estado = 'cancelado') / COUNT(*), 2) AS pct_cancelados
FROM tabla_final;

-- Q5: Medio de pago mas usado y cuantos pedidos tiene
SELECT
    medio_pago,
    COUNT(*) AS pedidos
FROM tabla_final
GROUP BY medio_pago
QUALIFY rank() OVER (ORDER BY COUNT(*) DESC) = 1;
