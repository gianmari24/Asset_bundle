CREATE OR REFRESH STREAMING TABLE dev.silver.sales_customers(
  CONSTRAINT valid_email EXPECT (email_address LIKE '%@%'),
  CONSTRAINT valid_gender EXPECT (gender IN ('male', 'female'))
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
FROM STREAM(dev.bronze.sales_customers)
WHERE customerid IS NOT NULL
  AND first_name IS NOT NULL
  AND last_name IS NOT NULL
  AND __END_AT IS NULL
