CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.sales_transactions_silver(
  CONSTRAINT valid_total_price EXPECT (total_price = unit_price * quantity) ON VIOLATION DROP ROW
)
TBLPROPERTIES (
'quality' = 'Silver'
)
COMMENT "Tabella silver delle transazioni con validazione e pulizia dati"
AS
SELECT
  transactionid AS transactionid,
  customerid AS customerid,
  franchiseid AS franchiseid,
  datetime AS transaction_datetime,
  TRIM(product) AS product_name,
  quantity,
  unitprice AS unit_price,
  totalprice AS total_price,
  TRIM(paymentmethod) AS payment_method,
  cardnumber AS card_number,
  updated_at
FROM ${catalog}.bronze.sales_transactions_bronze
