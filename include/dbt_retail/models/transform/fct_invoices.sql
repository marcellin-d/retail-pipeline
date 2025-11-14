-- fct_invoices.sql

WITH fct_invoices_cte AS (
    SELECT
        InvoiceNo AS invoice_id,
        InvoiceDate::date AS datetime_id,
        {{ dbt_utils.generate_surrogate_key(['StockCode', 'Description', 'UnitPrice']) }} AS product_id,
        {{ dbt_utils.generate_surrogate_key(['CustomerID', 'Country']) }} AS customer_id,
        Quantity AS quantity,
        UnitPrice as price,
        Quantity * UnitPrice AS total_amount
    FROM {{ source('retail', 'raw_invoices') }}
    WHERE Quantity > 0
)
SELECT
    fi.invoice_id,
    dt.datetime_id,
    fi.product_id,
    fi.customer_id,
    fi.quantity,
    fi.price,
    fi.total_amount
FROM fct_invoices_cte fi
INNER JOIN {{ ref('dim_datetime') }} dt ON fi.datetime_id = dt.datetime_id
INNER JOIN {{ ref('dim_product') }} dp ON fi.product_id = dp.product_id
INNER JOIN {{ ref('dim_customer') }} dc ON fi.customer_id = dc.customer_id
