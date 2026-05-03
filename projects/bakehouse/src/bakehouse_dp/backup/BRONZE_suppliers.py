from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ========== STEP 1: CREATE RAW STREAMING TABLE WITH AUTO LOADER ==========
@dp.table(name="suppliers_raw")
def suppliers_raw():
    """
    Reads CSV files from cloud storage using Auto Loader.
    Adds metadata columns: file_name (source file path) and file_timestamp (for SCD sequencing).
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        
        # ========== CSV FORMAT OPTIONS ==========
        .option("header", "true")  # First row as header (true/false)
        .option("delimiter", ",")  # Field delimiter, default: ','
        .option("quote", '"')  # Quote character, default: '"'
        .option("escape", "\\")  # Escape character, default: '\'
        .option("encoding", "UTF-8")  # Common values: 'UTF-8', 'ISO-8859-1', 'US-ASCII'
        .option("lineSep", "\n")  # Line separator, default: '\n'. Values: '\n', '\r', '\r\n'
        .option("cloudFiles.inferColumnTypes", "true")  # Infer schema flag if schema not explicit (true/false)
        # .option("cloudFiles.schemaHints", "col1 INT, col2 TIMESTAMP")  # Suggest some column types during inference
        .option("pathGlobFilter", "*")  # File filter pattern in addition to main path
        .option("dateFormat", "yyyy-MM-dd")  # Format date to parse, default: 'yyyy-MM-dd'
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")  # Format timestamp to parse, default: 'yyyy-MM-dd HH:mm:ss'
        .option("nullValue", "")  # String to consider as NULL, default: ''
        .option("ignoreLeadingWhiteSpace", "true")  # Remove space at the beginning (true/false)
        .option("ignoreTrailingWhiteSpace", "true")  # Remove space at the end (true/false)
        
        # ========== AUTO LOADER OPTIONS ==========
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # How to manage new columns: 'addNewColumns', 'rescue', 'failOnNewColumns'
        .option("rescuedDataColumn", "_rescued_data")  # Column name for data that doesn't respect correct format or new columns (if mode='rescue')
        
        # ========== PYTHON-SPECIFIC AUTO LOADER THROTTLING OPTIONS ==========
        # NOTE: These options are ONLY available in Python, NOT in SQL
        .option("cloudFiles.maxFilesPerTrigger", "1000")  # Maximum number of new files to process per trigger (batch). Useful for: Controlling processing rate, avoiding overloading clusters
        .option("cloudFiles.maxBytesPerTrigger", "10g")  # Maximum data volume to process per trigger.Useful for: Controlling memory usage, predictable batch sizes, cost control
        .option("cloudFiles.maxFileAge", "365 days")  #Ignore files older than specified age
        # Useful for: Skipping old/archived files, incremental migrations, focusing on recent data
        .option("cloudFiles.includeExistingFiles", "true")  # Process existing files on first run (true) or only new files (false). Default: true. Set to false to ignore historical files and only process files added after pipeline starts
        .load("/Volumes/dev/bronze/volume/input_files/samples_bakehouse_sales_suppliers_*.csv")
        .withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("file_timestamp", F.col("_metadata.file_modification_time"))
    )
