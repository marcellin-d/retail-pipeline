{{ config(
    materialized='table',       
    schema='report'         
) }}

WITH customer_sales AS (
    SELECT
        dc.customer_id,
        dc.country,
        dc.iso,
        COUNT(DISTINCT fi.invoice_id) AS total_invoices,
        SUM(fi.quantity) AS total_quantity,
        SUM(fi.total_amount) AS total_sales,
        ROUND(AVG(fi.total_amount), 2) AS avg_invoice_value,
        MIN(fi.datetime_id) AS first_purchase_date,
        MAX(fi.datetime_id) AS last_purchase_date
    FROM {{ ref('fct_invoices') }} fi
    INNER JOIN {{ ref('dim_customer') }} dc
        ON fi.customer_id = dc.customer_id
    GROUP BY
        dc.customer_id,
        dc.country,
        dc.iso
)

SELECT
    cs.customer_id,
    cs.country,
    cs.iso,
    cs.total_invoices,
    cs.total_quantity,
    cs.total_sales,
    cs.avg_invoice_value,
    cs.first_purchase_date,
    cs.last_purchase_date,
    ROUND(cs.total_sales / NULLIF(cs.total_invoices, 0), 2) AS avg_revenue_per_invoice
FROM customer_sales cs
ORDER BY cs.total_sales DESC
