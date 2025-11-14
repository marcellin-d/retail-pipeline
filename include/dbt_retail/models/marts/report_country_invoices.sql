{{ config(
    materialized='table',
    schema='report'
) }}

WITH country_sales AS (
    SELECT
        dc.country,
        dc.iso,
        COUNT(DISTINCT fi.invoice_id) AS total_invoices,
        SUM(fi.quantity) AS total_quantity,
        SUM(fi.total_amount) AS total_sales,
        ROUND(AVG(fi.total_amount), 2) AS avg_invoice_value
    FROM {{ ref('fct_invoices') }} fi
    INNER JOIN {{ ref('dim_customer') }} dc
        ON fi.customer_id = dc.customer_id
    GROUP BY dc.country, dc.iso
)

SELECT
    country,
    iso,
    total_invoices,
    total_quantity,
    total_sales,
    avg_invoice_value,
    ROUND(total_sales / NULLIF(total_invoices, 0), 2) AS avg_revenue_per_invoice
FROM country_sales
ORDER BY total_sales DESC
