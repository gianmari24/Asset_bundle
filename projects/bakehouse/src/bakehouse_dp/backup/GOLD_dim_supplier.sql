CREATE OR REFRESH MATERIALIZED VIEW dev.gold.dim_supplier
COMMENT "Dimensione fornitori con solo record validi"
AS
SELECT
  supplierID AS supplier_ID,
  name,
  ingredient,
  continent,
  city,
  district,
  size,
  longitude,
  latitude,
  approved
FROM dev.silver.suppliers
WHERE supplierID IS NOT NULL
  AND approved = 'Y'
  AND __END_AT IS NULL
