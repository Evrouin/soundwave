"""Bronze-to-Silver ETL: deduplicate, enforce schema, validate, SCD Type 2 for artists."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, md5, row_number
from pyspark.sql.types import FloatType, StringType, TimestampType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()

SILVER_TRACKS = "delta.`s3a://soundwave/silver/tracks`"
SILVER_ARTISTS = "delta.`s3a://soundwave/silver/artists`"
RANGE_COLS = ["danceability", "energy", "valence", "acousticness", "instrumentalness"]

# --- Read & deduplicate bronze ---
bronze = spark.read.format("delta").load("s3a://soundwave/bronze/tracks")
dedup_window = Window.partitionBy("track_id").orderBy(col("ingestion_timestamp").desc())
deduped = bronze.withColumn("_rn", row_number().over(dedup_window)).filter("_rn = 1").drop("_rn")

# --- Enforce canonical schema (snake_case, proper types) ---
canonical = deduped.select(
    col("track_id").cast(StringType()),
    col("track_name").cast(StringType()),
    col("artist_id").cast(StringType()),
    col("artist_name").cast(StringType()),
    col("danceability").cast(FloatType()),
    col("energy").cast(FloatType()),
    col("valence").cast(FloatType()),
    col("acousticness").cast(FloatType()),
    col("instrumentalness").cast(FloatType()),
    col("tempo").cast(FloatType()),
    col("ingestion_timestamp").cast(TimestampType()),
)

# --- Validate audio feature ranges ---
for c in RANGE_COLS:
    canonical = canonical.filter((col(c) >= 0.0) & (col(c) <= 1.0))
canonical = canonical.filter(col("tempo").isNotNull())

# --- Write silver tracks ---
canonical.write.format("delta").mode("overwrite").save("s3a://soundwave/silver/tracks")

# --- SCD Type 2 for artist dimension ---
new_artists = (
    canonical.select("artist_id", "artist_name")
    .distinct()
    .withColumn("row_hash", md5(concat_ws("||", "artist_id", "artist_name")))
)

try:
    existing = spark.read.format("delta").load("s3a://soundwave/silver/artists")
    active = existing.filter(col("is_current"))
    changed = (
        new_artists.alias("n")
        .join(active.alias("e"), "artist_id", "left")
        .filter((col("e.row_hash").isNull()) | (col("e.row_hash") != col("n.row_hash")))
    )
    # Expire old rows
    expire_ids = changed.select("artist_id").distinct()
    expired = (
        existing.join(expire_ids, "artist_id", "left_semi")
        .withColumn("is_current", lit(False))
        .withColumn("end_ts", current_timestamp())
    )
    unchanged = existing.join(expire_ids, "artist_id", "left_anti")
    # New active rows
    inserts = (
        changed.select(col("n.artist_id"), col("n.artist_name"), col("n.row_hash"))
        .withColumn("is_current", lit(True))
        .withColumn("start_ts", current_timestamp())
        .withColumn("end_ts", lit(None).cast(TimestampType()))
    )
    unchanged.unionByName(expired, allowMissingColumns=True) \
        .unionByName(inserts, allowMissingColumns=True) \
        .write.format("delta").mode("overwrite").save("s3a://soundwave/silver/artists")
except Exception:
    # First run — bootstrap artist dimension
    (
        new_artists
        .withColumn("is_current", lit(True))
        .withColumn("start_ts", current_timestamp())
        .withColumn("end_ts", lit(None).cast(TimestampType()))
        .write.format("delta").mode("overwrite").save("s3a://soundwave/silver/artists")
    )

spark.stop()
