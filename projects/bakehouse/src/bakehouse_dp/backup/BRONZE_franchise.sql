CREATE OR REFRESH STREAMING TABLE franchises_raw
-- For explicit schema specify column names and types into CREATE TABLE:
/* (col1 INT, col2 TIMESTAMP, ... ) */
AS 
SELECT 
  *,
  _metadata.file_path AS file_name,  -- Input File Name from which record comes
  _metadata.file_modification_time AS file_timestamp  -- Timestamp field for sequencing scd2
FROM STREAM read_files(
  '/Volumes/dev/bronze/volume/input_files/samples_bakehouse_sales_franchises_*.csv',  
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
  schemaLocation => '/Volumes/dev/bronze/volume/schema/', -- schemaLocation: path where to store schema infered
  schemaEvolutionMode => 'addNewColumns', -- schemaEvolutionMode: how to manage new columns
  -- possible values: 'addNewColumns' (add colonne), 'rescue' (store in _rescued_data), 'failOnNewColumns' (fail)
  rescuedDataColumn => '_rescued_data' -- rescuedDataColumn: Column name for data that not respect correct format or new columns (if mode='rescue')
  -- NOTE: maxFilesPerTrigger, maxBytesPerTrigger, maxFileAge are NOT supported in SQL - use Python if needed
);