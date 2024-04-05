from pyspark.sql import SparkSession

# Créer la session Spark
spark = SparkSession.builder.appName("KafkaStreaming").getOrCreate()

# Muter les logs inférieurs au niveau Warning
spark.sparkContext.setLogLevel("WARN")

# Définir le topic Kafka à écouter
kafka_topic_name = "topic-tp_group"

# Définir les serveurs Kafka Bootstrap
kafka_bootstrap_servers = 'kafka:9092'

# Charger les données du stream Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# Convertir les données en chaînes de caractères pour les rendre utilisables
df = df.selectExpr("CAST(value AS STRING)")

# Écrire les données du stream sur la console
query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Attendre que le streaming se termine
query.awaitTermination()
