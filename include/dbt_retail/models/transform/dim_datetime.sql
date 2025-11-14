-- dim_datetime.sql

WITH date_cte AS (
  SELECT DISTINCT
    InvoiceDate::date AS datetime_id
  FROM {{ source('retail', 'raw_invoices') }}
  WHERE InvoiceDate IS NOT NULL
)
SELECT
  datetime_id,
  EXTRACT(YEAR FROM datetime_id) AS year,
  EXTRACT(MONTH FROM datetime_id) AS month,
  EXTRACT(DAY FROM datetime_id) AS day
FROM date_cte
