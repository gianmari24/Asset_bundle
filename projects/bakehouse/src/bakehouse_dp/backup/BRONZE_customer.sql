CREATE OR REFRESH STREAMING TABLE customers_raw
AS SELECT * 
FROM STREAM(samples.bakehouse.sales_customers)
;
