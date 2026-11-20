# Informe de calidad de datos - Pedidos online Valhalla S.A.

*Generado automaticamente por el pipeline el 18/11/2026 10:00.*

## 1. Que se hizo
Pipeline ETL con Python + DuckDB (`pipeline.py`, SQL en `sql/`) orquestado con un DAG de Airflow
(`dags/valhalla_pedidos_dag.py`, cron `0 2 * * *`, todos los dias a las 2:00 AM).
El archivo `datos/pedidos.csv` nunca se modifica: se lee, se copia a `raw_pedidos` tal cual y
todo el trabajo posterior ocurre en `valhalla.duckdb`.

## 2. Resultado de la calidad
| Etapa | Filas |
|---|---|
| Leidas del CSV (`raw_pedidos`) | 2.575 (2.500 `id_pedido` distintos) |
| Validas a la primera (`validos`) | 1.475 |
| Rechazadas (`rechazados`) | 1.100 |
| Recuperadas tras limpiar (`rechazados_limpios`) | 1.073 |
| Descartadas (`descartados`) | 27 |
| **Pedidos confiables (`tabla_final`)** | **2.500** |

Duplicados eliminados al unir validos con recuperados: 48.

## 3. Reglas de validacion (15) y filas que las incumplen
Una fila puede incumplir varias reglas a la vez.

| Regla | Que controla | Filas |
|---|---|---|
| `id_pedido_no_nulo` | id_pedido obligatorio | 0 |
| `id_pedido_unico` | id_pedido no repetido (se conserva la primera aparicion) | 75 |
| `fecha_valida` | fecha en formato YYYY-MM-DD, real y no futura | 257 |
| `id_cliente_no_nulo` | id_cliente obligatorio | 0 |
| `email_valido` | email con formato usuario@dominio.ext | 0 |
| `pais_no_nulo` | pais de entrega obligatorio | 126 |
| `ciudad_no_nula` | ciudad de entrega obligatoria | 0 |
| `categoria_no_nula` | categoria del producto obligatoria | 101 |
| `producto_no_nulo` | producto obligatorio | 0 |
| `cantidad_positiva` | cantidad entera mayor a 0 | 102 |
| `precio_numerico_positivo` | precio_unitario numerico (punto decimal) mayor a 0 | 130 |
| `descuento_en_rango` | descuento_pct entre 0 y 100 | 0 |
| `total_coherente` | total presente y igual a cantidad*precio*(1-descuento/100) | 231 |
| `estado_valido` | estado exacto: entregado/enviado/pendiente/cancelado | 205 |
| `medio_pago_valido` | medio_pago exacto: tarjeta/transferencia/efectivo/billetera_virtual | 0 |

## 4. Limpieza aplicada a los rechazados
Fechas `dd/mm/yyyy` a ISO; precios `210.000,00` a numero; cantidades negativas a valor absoluto
(en todos los casos el total original coincidia con el valor absoluto); total recalculado como
`cantidad * precio * (1 - descuento/100)`; estado y medio de pago en minusculas y sin espacios;
pais faltante completado desde la ciudad y categoria faltante desde el producto.

## 5. Respuestas a las preguntas de negocio
**Q1: Las 3 categorias con mayor facturacion total (excluyendo cancelados)**

| categoria | facturacion_total |
|---|---|
| Electronica | 958.481.000,00 |
| Hogar | 156.808.170,26 |
| Indumentaria | 144.170.950,00 |

**Q2: Facturacion por mes (excluyendo cancelados)**

| mes | pedidos | facturacion |
|---|---|---|
| 2024-01 | 72 | 58.077.209,38 |
| 2024-02 | 91 | 42.277.751,47 |
| 2024-03 | 82 | 33.894.131,37 |
| 2024-04 | 87 | 43.168.104,13 |
| 2024-05 | 92 | 61.579.862,28 |
| 2024-06 | 83 | 44.971.914,33 |
| 2024-07 | 78 | 32.626.382,11 |
| 2024-08 | 98 | 76.077.787,94 |
| 2024-09 | 92 | 41.954.300,28 |
| 2024-10 | 95 | 57.351.319,19 |
| 2024-11 | 91 | 55.446.984,03 |
| 2024-12 | 103 | 48.632.169,88 |
| 2025-01 | 92 | 63.870.692,88 |
| 2025-02 | 82 | 51.812.723,25 |
| 2025-03 | 91 | 55.442.105,66 |
| 2025-04 | 73 | 53.749.220,93 |
| 2025-05 | 97 | 52.685.443,12 |
| 2025-06 | 80 | 47.002.137,58 |
| 2025-07 | 106 | 71.185.573,79 |
| 2025-08 | 88 | 62.642.510,25 |
| 2025-09 | 78 | 56.948.708,68 |
| 2025-10 | 89 | 74.817.872,55 |
| 2025-11 | 88 | 70.656.642,27 |
| 2025-12 | 92 | 43.788.432,06 |

**Q3: Ticket promedio (total promedio por pedido) por pais (excluyendo cancelados)**

| pais | pedidos | ticket_promedio |
|---|---|---|
| Chile | 244 | 707.139,64 |
| Colombia | 264 | 699.850,60 |
| Argentina | 808 | 615.858,57 |
| Uruguay | 269 | 614.433,49 |
| Peru | 265 | 556.445,64 |
| Paraguay | 270 | 492.603,42 |

**Q4: Porcentaje de pedidos cancelados (sobre todos los pedidos)**

| pedidos_totales | pedidos_cancelados | pct_cancelados |
|---|---|---|
| 2.500 | 380 | 15,20 |

**Q5: Medio de pago mas usado y cuantos pedidos tiene**

| medio_pago | pedidos |
|---|---|
| transferencia | 661 |

## 6. Supuestos
- Facturacion = suma de `total` sin pedidos cancelados (se aplica a Q1, Q2 y Q3). Q4 usa todos los pedidos.
- Fechas con barra se interpretan como dia/mes/anio.
- Una cantidad negativa se considera un error de signo.
- Los totales se recalculan a partir de cantidad, precio y descuento.
