{{ config(
    materialized='table',     
    schema='report'
) }}

WITH monthly_sales AS (
    SELECT
        dt.year,
        dt.month,
        COUNT(DISTINCT fi.invoice_id) AS total_invoices,
        SUM(fi.quantity) AS total_quantity,
        SUM(fi.total_amount) AS total_sales,
        ROUND(AVG(fi.total_amount), 2) AS avg_invoice_value
    FROM {{ ref('fct_invoices') }} fi
    INNER JOIN {{ ref('dim_datetime') }} dt
        ON fi.datetime_id = dt.datetime_id
    GROUP BY dt.year, dt.month
)

SELECT
    year,
    month,
    total_invoices,
    total_quantity,
    total_sales,
    avg_invoice_value,
    ROUND(total_sales / NULLIF(total_invoices, 0), 2) AS avg_revenue_per_invoice
FROM monthly_sales
ORDER BY year, month
