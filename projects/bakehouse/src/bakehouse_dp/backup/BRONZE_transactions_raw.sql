CREATE OR REFRESH STREAMING TABLE transactions_raw
AS SELECT * FROM STREAM(samples.bakehouse.sales_transactions);