CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.fact_transactions_sales_denormalized_gold
COMMENT "Vista denormalizzata delle vendite con informazioni cliente e franchise"
TBLPROPERTIES (
'quality' = 'Gold'
)
AS
SELECT
  -- Chiavi e date
  fs.customer_ID,
  fs.franchise_ID,
  fs.product_name,
  
  -- Metriche vendite
  fs.transaction_count,
  fs.total_quantity,
  fs.total_revenue,
  fs.avg_unit_price,
  fs.sale_date,
  fs.first_purchase,
  fs.last_purchase,
  
  -- Dati cliente (campi principali)
  c.first_name AS customer_first_name,
  c.last_name AS customer_last_name,
  c.city AS customer_city,
  c.country AS customer_country,
  
  -- Dati franchise (campi principali)
  f.name AS franchise_name,
  f.city AS franchise_city,
  f.country AS franchise_country

FROM ${catalog}.gold.fact_transactions_sales_gold fs
INNER JOIN ${catalog}.gold.dim_customer_gold c
  ON fs.customer_id = c.customer_id
INNER JOIN ${catalog}.gold.dim_franchise_gold f
  ON fs.franchise_id = f.franchise_id
