-- ========== SCD TYPE 2 IMPLEMENTATION ==========
-- 1. Create the target streaming table for SCD Type 2. The __START_AT and __END_AT columns are automatically added by Auto CDC
-- If you define an explicit schema, you must manually include __START_AT and __END_AT using the same data type as the SEQUENCE BY column

CREATE OR REFRESH STREAMING TABLE silver.franchises(
  CONSTRAINT valid_lat EXPECT (ABS(latitude) <= 90) ON VIOLATION DROP ROW,
  CONSTRAINT valid_lon EXPECT (ABS(longitude) <= 180),
  CONSTRAINT id_not_null EXPECT (franchiseID IS NOT NULL) ON VIOLATION FAIL UPDATE
);

-- 2. AUTO CDC FLOW
CREATE FLOW franchises_cdc AS 
AUTO CDC INTO silver.franchises
FROM STREAM franchises_raw
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
