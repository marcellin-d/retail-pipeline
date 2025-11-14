{{ config(
    materialized='table', 
    schema='report'     
) }}

WITH product_sales AS (
    SELECT
        dp.product_id,
        dp.stock_code,
        dp.description,
        SUM(fi.quantity) AS total_quantity_sold,
        SUM(fi.total_amount) AS total_sales,
        ROUND(AVG(fi.price), 2) AS avg_price
    FROM {{ ref('fct_invoices') }} fi
    INNER JOIN {{ ref('dim_product') }} dp
        ON fi.product_id = dp.product_id
    GROUP BY
        dp.product_id,
        dp.stock_code,
        dp.description
)

SELECT
    ps.product_id,
    ps.stock_code,
    ps.description,
    ps.total_quantity_sold,
    ps.total_sales,
    ps.avg_price
FROM product_sales ps
ORDER BY ps.total_sales DESC
LIMIT 10
