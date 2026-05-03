CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.sales_customers_silver(
  CONSTRAINT valid_email EXPECT (email_address LIKE '%@%'),
  CONSTRAINT valid_gender EXPECT (gender IN ('male', 'female'))
)
TBLPROPERTIES (
'quality' = 'Silver'
)
COMMENT "Tabella silver dei clienti con validazione e pulizia dati"
AS
SELECT
  customerid AS customerid,
  TRIM(first_name) AS first_name,
  TRIM(last_name) AS last_name,
  TRIM(email_address) AS email_address,
  TRIM(phone_number) AS phone_number,
  TRIM(address) AS address,
  TRIM(city) AS city,
  TRIM(state) AS state,
  TRIM(country) AS country,
  TRIM(continent) AS continent,
  postal_zip_code,
  LOWER(TRIM(gender)) AS gender,
  updated_at
FROM ${catalog}.bronze.sales_customers_bronze
WHERE customerid IS NOT NULL
  AND first_name IS NOT NULL
  AND last_name IS NOT NULL
  AND __END_AT IS NULL
  ;



CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.dim_customer_gold
COMMENT "Dimensione clienti con solo record validi"
TBLPROPERTIES (
'quality' = 'Gold'
)
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
FROM ${catalog}.silver.sales_customers_silver
WHERE customerid IS NOT NULL
;
