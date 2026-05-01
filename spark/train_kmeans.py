"""Train K-Means (k=6) on silver audio features, assign mood labels, save to MinIO."""

import sys

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

version = sys.argv[1] if len(sys.argv) > 1 else "1"
FEATURE_COLS = ["danceability", "energy", "valence", "acousticness", "instrumentalness", "tempo"]
MOOD_LABELS = ["energetic", "melancholic", "hype", "chill", "acoustic", "intense"]
MODEL_PATH = f"s3a://soundwave/models/kmeans/v{version}/"

spark = SparkSession.builder.appName("train_kmeans").getOrCreate()
silver = spark.read.format("delta").load("s3a://soundwave/silver/tracks").na.drop(subset=FEATURE_COLS)

# --- Normalize features ---
assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features")
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
assembled = assembler.transform(silver)
scaler_model = scaler.fit(assembled)
scaled = scaler_model.transform(assembled)

# --- Train K-Means ---
kmeans = KMeans(k=6, seed=42, featuresCol="features", predictionCol="cluster")
model = kmeans.fit(scaled)

# --- Assign mood labels based on centroid feature dominance ---
centroids = model.clusterCenters()
# Map: index of dominant feature -> mood (energy->energetic, valence low->melancholic, etc.)
FEATURE_MOOD = {0: "hype", 1: "energetic", 2: "chill", 3: "acoustic", 4: "acoustic", 5: "intense"}
cluster_mood = {}
assigned_moods = set()
for i, c in enumerate(centroids):
    dominant = int(c.argmax())
    label = FEATURE_MOOD.get(dominant, MOOD_LABELS[i % len(MOOD_LABELS)])
    while label in assigned_moods:
        label = [m for m in MOOD_LABELS if m not in assigned_moods][0]
    cluster_mood[i] = label
    assigned_moods.add(label)

mood_map = spark.sparkContext.broadcast(cluster_mood)
assign_mood = udf(lambda cluster: mood_map.value.get(cluster, "unknown"), StringType())

predictions = model.transform(scaled).withColumn("mood_label", assign_mood(col("cluster")))
(
    predictions.select("track_id", "mood_label", *FEATURE_COLS)
    .write.format("delta").mode("overwrite").save("s3a://soundwave/gold/mood_predictions")
)

# --- Save model ---
model.write().overwrite().save(MODEL_PATH)
print(f"Model saved to {MODEL_PATH}")

spark.stop()
