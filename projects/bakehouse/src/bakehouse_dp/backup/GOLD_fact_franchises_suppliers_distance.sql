CREATE OR REFRESH MATERIALIZED VIEW dev.gold.fact_franchises_suppliers_distance
COMMENT "Distanza geografica tra franchise e fornitori usando formula Haversine"
AS
SELECT
  f.franchiseid as franchise_ID,
  s.supplierID as supplier_ID,
  -- Formula di Haversine per calcolare distanza in km
  6371 * 2 * ASIN(SQRT(
    POW(SIN(RADIANS(s.latitude - f.latitude) / 2), 2) +
    COS(RADIANS(f.latitude)) * COS(RADIANS(s.latitude)) *
    POW(SIN(RADIANS(s.longitude - f.longitude) / 2), 2)
  )) AS distance_km
FROM dev.silver.franchises f
CROSS JOIN dev.silver.suppliers s
WHERE f.franchiseid IS NOT NULL
  AND s.supplierID IS NOT NULL
  AND f.latitude IS NOT NULL
  AND f.longitude IS NOT NULL
  AND s.latitude IS NOT NULL
  AND s.longitude IS NOT NULL
  AND s.approved = 'Y'
  AND f.__END_AT IS NULL
  AND s.__END_AT IS NULL
