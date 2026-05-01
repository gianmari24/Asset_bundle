CREATE OR REFRESH MATERIALIZED VIEW dev.gold.dim_franchise
COMMENT "Dimensione franchise con solo record validi"
AS
SELECT
  franchiseid AS franchise_id,
  name,
  city,
  district,
  zipcode,
  country,
  size,
  longitude,
  latitude
FROM dev.silver.franchises
WHERE franchiseid IS NOT NULL
AND __END_AT IS NULL
