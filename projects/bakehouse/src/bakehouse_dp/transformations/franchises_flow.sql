-- ========== AUTOLOADER - FILE INGESTION ==========
CREATE OR REFRESH STREAMING TABLE franchises_raw_bronze
-- For explicit schema specify column names and types into CREATE TABLE:
/* (col1 INT, col2 TIMESTAMP, ... ) */
TBLPROPERTIES (
'quality' = 'Bronze'
)
AS 
SELECT 
  *,
  _metadata.file_path AS file_name,  -- Input File Name from which record comes
  _metadata.file_modification_time AS file_timestamp  -- Timestamp field for sequencing scd2
FROM STREAM read_files(
  '/Volumes/${catalog}/bronze/volume/input_files/samples_bakehouse_sales_franchises_*.csv',  
  format => 'csv', -- file format: 'csv', 'json', 'parquet', 'avro', 'orc', 'text', 'binaryFile'
  header => true, -- header: first row as header (true/false)
  delimiter => ',', -- delimiter: field delimiter, default: ','
  quote => '"', -- Quote: quote charachter, default: '"'
  escape => '\\', -- escape: escape character, default: '\'
  encoding => 'UTF-8', -- encoding: common values: 'UTF-8', 'ISO-8859-1', 'US-ASCII'
  lineSep => '\n', -- lineSep: line separator, default: '\n'. Values: '\n', '\r', '\r\n'
  inferColumnTypes => true, -- inferColumnTypes: inferschema flag if schema not explicit (true/false)
  --schemaHints => 'col1 INT, col2 TIMESTAMP', -- schemaHints: suggest some column types during inference
  pathGlobFilter => '*', -- pathGlobFilter: file filter pattern in addition to main path
  dateFormat => 'yyyy-MM-dd', -- dateFormat: format date to parse, default: 'yyyy-MM-dd'
  timestampFormat => 'yyyy-MM-dd HH:mm:ss', -- timestampFormat: format timestamp to parse, default: 'yyyy-MM-dd HH:mm:ss'
  nullValue => '', -- nullValue: string to consider as NULL, default: ''
  ignoreLeadingWhiteSpace => true, -- ignoreLeadingWhiteSpace: Remove space at the beginning (true/false)
  ignoreTrailingWhiteSpace => true, -- ignoreTrailingWhiteSpace: Remove space at the end (true/false)
  -- ========== AUTO LOADER OPTIONS ==========
  schemaLocation => '/Volumes/${catalog}/bronze/volume/schema/', -- schemaLocation: path where to store schema infered
  schemaEvolutionMode => 'rescue', -- schemaEvolutionMode: how to manage new columns
  -- possible values: 'addNewColumns' (add colonne), 'rescue' (store in _rescued_data), 'failOnNewColumns' (fail)
  rescuedDataColumn => '_rescued_data' -- rescuedDataColumn: Column name for data that not respect correct format or new columns (if mode='rescue')
  -- NOTE: maxFilesPerTrigger, maxBytesPerTrigger, maxFileAge are NOT supported in SQL - use Python if needed
);


-- ========== SCD TYPE 2 IMPLEMENTATION ==========
-- 1. Create the target streaming table for SCD Type 2. The __START_AT and __END_AT columns are automatically added by Auto CDC
-- If you define an explicit schema, you must manually include __START_AT and __END_AT using the same data type as the SEQUENCE BY column

CREATE OR REFRESH STREAMING TABLE silver.franchises_silver(
  CONSTRAINT valid_lat EXPECT (ABS(latitude) <= 90) ON VIOLATION DROP ROW,
  CONSTRAINT valid_lon EXPECT (ABS(longitude) <= 180),
  CONSTRAINT id_not_null EXPECT (franchiseID IS NOT NULL) ON VIOLATION FAIL UPDATE
)
TBLPROPERTIES (
'quality' = 'Silver'
);

-- 2. AUTO CDC FLOW
CREATE FLOW franchises_cdc AS 
AUTO CDC INTO silver.franchises_silver
FROM STREAM franchises_raw_bronze
KEYS (franchiseID) -- Primary Key

-- SEQUENCE BY: Column used to order changes and handle out-of-order events. Events with an older sequence value are ignored if they arrive late. To order by multiple columns (e.g. timestamp + ID as a tie-breaker): 
-- SEQUENCE BY STRUCT(timestamp_col, id_col)
SEQUENCE BY file_timestamp

-- COLUMNS: Specifies which columns to include in the target table.
-- Default: all columns from the source.
-- Syntax: COLUMNS col1, col2, col3, ... or COLUMNS * EXCEPT (excluded_col1, excluded_col2, ...)
COLUMNS * EXCEPT (file_timestamp, _rescued_data)

-- STORED AS: Type of SCD (Slowly Changing Dimension).
-- SCD TYPE 1: Keeps only the current version (upsert). Updates overwrite previous values.
-- SCD TYPE 2: Preserves full history. Creates a new row for each version using __START_AT and __END_AT columns.
--             __END_AT = NULL indicates the current/active version.
STORED AS SCD TYPE 2


-- TRACK HISTORY ON (OPTIONAL – SCD TYPE 2 only): Specifies which columns to track historically.
-- Default: all columns.
-- If a subset is specified, changes to other columns are applied in-place without creating new historical versions (reduces storage and query complexity).
-- Example: TRACK HISTORY ON name, city (track only name and city)
-- Example: TRACK HISTORY ON * EXCEPT (city) (track all except city)
-- TRACK HISTORY ON * EXCEPT (city)

-- IGNORE NULL UPDATES --(OPTIONAL) Ignore updates where all non-key columns are NULL. Useful to filter out empty or partially populated records.

-- APPLY AS DELETE WHEN -- (OPTIONAL) Condition used to identify records to be deleted.
  -- Example: APPLY AS DELETE WHEN is_deleted = true
  -- For SCD Type 2: marks the record as expired (sets __END_AT).
  -- For SCD Type 1: physically removes the record.

-- APPLY AS TRUNCATE WHEN -- (OPTIONAL) Condition used to truncate (empty) the target table.
-- When the condition evaluates to true, all records are deleted before applying new changes.
-- Example: APPLY AS TRUNCATE WHEN operation = 'TRUNCATE'
-- Supported ONLY for SCD Type 1 (causes an error for SCD Type 2).
;


CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.dim_franchise_gold
COMMENT "Dimensione franchise con solo record validi"
TBLPROPERTIES (
'quality' = 'Gold'
)
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
FROM ${catalog}.silver.franchises_silver
WHERE franchiseid IS NOT NULL
AND __END_AT IS NULL

