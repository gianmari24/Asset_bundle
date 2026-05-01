from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")

# ========== STEP 1: CREATE RAW STREAMING TABLE WITH AUTO LOADER ==========
@dp.table(name="suppliers_raw_bronze",
          table_properties={
              "quality": "Bronze"
          }
)
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
        .load(f"/Volumes/{catalog}/bronze/volume/input_files/samples_bakehouse_sales_suppliers_*.csv")
        .withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("file_timestamp", F.col("_metadata.file_modification_time"))
    )


# ========== STEP 2: TEMPORARY VIEW WITH TRIM FOR TEXT FIELDS ==========
@dp.temporary_view(name="vw_suppliers_cleaned")
def suppliers_cleaned():
    """
    Applies TRIM to all text fields before CDC processing.
    """
    return (
        spark.readStream.table("suppliers_raw_bronze")
        .withColumn("name", F.trim(F.col("name")))
        .withColumn("ingredient", F.trim(F.col("ingredient")))
        .withColumn("continent", F.trim(F.col("continent")))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("district", F.trim(F.col("district")))
        .withColumn("size", F.trim(F.col("size")))
        .withColumn("approved", F.trim(F.col("approved")))
        .withColumn("file_name", F.trim(F.col("file_name")))
    )

# ========== STEP 3: QUARANTINE TABLE FOR INVALID RECORDS ==========
@dp.table(
    name="bronze.suppliers_invalid_bronze",
    comment="Suppliers records with NULL supplierID sent to quarantine",
    table_properties={
    "quality": "Bronze"
    }
)
def suppliers_quarantine():
    return (
        spark.readStream.table("suppliers_raw_bronze")
        .filter("supplierID IS NULL")
        .withColumn("quarantine_reason", F.lit("supplierID IS NULL"))
        .withColumn("quarantine_timestamp", F.current_timestamp())
    )

# ========== STEP 4: CREATE TARGET STREAMING TABLE WITH EXPECT_OR_DROP ==========
dp.create_streaming_table(
    name="silver.suppliers_silver",
    #expect_all, expect_all_or_fail, expect_all_or_drop
    expect_all_or_drop={
        "id_not_null": "supplierID IS NOT NULL"
    },
    table_properties={
    "quality": "Silver"
    }
)

# ========== STEP 5: AUTO CDC FLOW FOR SCD TYPE 2 ==========
dp.create_auto_cdc_flow(
    target="silver.suppliers_silver",
    source="vw_suppliers_cleaned",  # Reads from cleaned view with TRIM applied
    keys=["supplierID"],
    sequence_by="file_timestamp",
    except_column_list=["file_timestamp", "_rescued_data"],
    stored_as_scd_type=2
)

# ========== STEP 6: GOLD TABLE ==========
@dp.materialized_view(
    name=f"{catalog}.gold.dim_supplier_gold",
    comment="Dimensione fornitori con solo record validi",
    table_properties={
    "quality": "Gold"
    }
)
def dim_supplier():
    """
    Gold layer dimension table with only valid and approved suppliers.
    Filters SCD Type 2 table to get current records only (__END_AT IS NULL).
    """
    return (
        spark.read.table("silver.suppliers_silver")
        .filter("supplierID IS NOT NULL AND approved = 'Y' AND __END_AT IS NULL")
        .select(
            F.col("supplierID").alias("supplier_ID"),
            "name",
            "ingredient",
            "continent",
            "city",
            "district",
            "size",
            "longitude",
            "latitude",
            "approved"
        )
    )

