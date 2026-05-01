CREATE OR REFRESH MATERIALIZED VIEW dev.gold.dim_customer
COMMENT "Dimensione clienti con solo record validi"
AS
SELECT
  customerid AS customer_ID,
  first_name,
  last_name,
  email_address,
  phone_number,
  address,
  city,
  state,
  country,
  continent,
  postal_zip_code,
  gender,
  updated_at
FROM dev.silver.sales_customers
WHERE customerid IS NOT NULL
