CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.fact_transactions_sales_gold
COMMENT "Tabella dei fatti con vendite aggregate per cliente, franchise e prodotto"
TBLPROPERTIES (
'quality' = 'Gold'
)
AS
SELECT
  t.customerid AS customer_ID,
  t.franchiseid AS franchise_ID,
  t.product_name,
  date(t.transaction_datetime) AS sale_date,
  year(t.transaction_datetime) AS sale_year,
  month(t.transaction_datetime) AS sale_month,
  COUNT(t.transactionid) AS transaction_count,
  SUM(t.quantity) AS total_quantity,
  SUM(t.total_price) AS total_revenue,
  AVG(t.unit_price) AS avg_unit_price,
  MIN(t.transaction_datetime) AS first_purchase,
  MAX(t.transaction_datetime) AS last_purchase
FROM ${catalog}.silver.sales_transactions_silver t
WHERE t.transactionid IS NOT NULL
  AND t.customerid IS NOT NULL
  AND t.franchiseid IS NOT NULL
GROUP BY t.customerid, t.franchiseid, t.product_name, DATE(t.transaction_datetime)
