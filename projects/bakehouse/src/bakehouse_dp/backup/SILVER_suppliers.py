from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ========== STEP 2: TEMPORARY VIEW WITH TRIM FOR TEXT FIELDS ==========
@dp.temporary_view(name="suppliers_cleaned")
def suppliers_cleaned():
    """
    Applies TRIM to all text fields before CDC processing.
    """
    return (
        spark.readStream.table("suppliers_raw")
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
    name="bronze.suppliers_invalid",
    comment="Suppliers records with NULL supplierID sent to quarantine"
)
def suppliers_quarantine():
    return (
        spark.readStream.table("suppliers_raw")
        .filter("supplierID IS NULL")
        .withColumn("quarantine_reason", F.lit("supplierID IS NULL"))
        .withColumn("quarantine_timestamp", F.current_timestamp())
    )

# ========== STEP 4: CREATE TARGET STREAMING TABLE WITH EXPECT_OR_DROP ==========
dp.create_streaming_table(
    name="silver.suppliers",
    expect_all_or_drop={
        "id_not_null": "supplierID IS NOT NULL"
    }
)

# ========== STEP 5: AUTO CDC FLOW FOR SCD TYPE 2 ==========
dp.create_auto_cdc_flow(
    target="silver.suppliers",
    source="suppliers_cleaned",  # Reads from cleaned view with TRIM applied
    keys=["supplierID"],
    sequence_by="file_timestamp",
    except_column_list=["file_timestamp", "_rescued_data"],
    stored_as_scd_type=2
)
