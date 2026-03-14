from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys

input_311   = sys.argv[1]
input_pluto = sys.argv[2]
output_dir  = sys.argv[3]

spark = (
    SparkSession.builder
    .appName("NYC 311 Pipeline")
    .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Load 311
print("Loading 311 data...")
requests = spark.read.csv(input_311, header=True, inferSchema=True)

requests = requests.select(
    F.col("Unique Key").alias("unique_key"),
    F.col("Created Date").alias("created_date"),
    F.col("Closed Date").alias("closed_date"),
    F.col("Problem (formerly Complaint Type)").alias("complaint_type"),
    F.col("Problem Detail (formerly Descriptor)").alias("descriptor"),
    F.col("Incident Zip").alias("zip_code"),
    F.col("Borough").alias("borough"),
    F.col("BBL").cast("long").alias("bbl"),
    F.col("Community Board").alias("community_board"),
    F.col("Status").alias("status"),
    F.col("Latitude").alias("latitude"),
    F.col("Longitude").alias("longitude")
)

# Clean 311
print("Cleaning 311 data...")
requests = (
    requests
    .withColumn("created_date", F.to_timestamp("created_date", "MM/dd/yyyy hh:mm:ss a"))
    .withColumn("closed_date",  F.to_timestamp("closed_date",  "MM/dd/yyyy hh:mm:ss a"))
)
requests = requests.filter(F.year("created_date") >= 2020)
requests = requests.filter(
    F.col("bbl").isNotNull() &
    F.col("latitude").isNotNull() &
    F.col("longitude").isNotNull()
)
requests = requests.withColumn("borough", F.upper(F.trim(F.col("borough"))))
requests = requests.filter(
    F.col("borough").isin(["MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"])
)
requests = requests.filter(
    F.col("complaint_type").isNotNull() &
    (F.upper(F.trim(F.col("complaint_type"))) != "UNSPECIFIED") &
    (~F.upper(F.trim(F.col("complaint_type"))).like("%TEST%"))
)
requests = requests.dropDuplicates(["unique_key"])

# Load PLUTO
print("Loading PLUTO data...")
pluto = spark.read.csv(input_pluto, header=True, inferSchema=True)
pluto = pluto.select(
    F.col("bbl").cast("long").alias("bbl"),
    F.col("zipcode").alias("building_zip"),
    F.col("bldgclass").alias("building_class"),
    F.col("landuse").alias("land_use"),
    F.col("yearbuilt").alias("year_built"),
    F.col("numfloors").alias("num_floors"),
    F.col("unitsres").alias("units_residential"),
    F.col("bldgarea").alias("building_area"),
    F.col("ownername").alias("owner_name"),
    F.col("council").alias("council_district")
).filter(F.col("bbl").isNotNull())

# Join
print("Joining on BBL...")
joined = requests.join(pluto, on="bbl", how="left")

# Write
print(f"Writing to {output_dir}...")
joined.write.mode("overwrite").parquet(output_dir)
count = joined.count()
print(f"Done! Total rows written: {count:,}")
spark.stop()
