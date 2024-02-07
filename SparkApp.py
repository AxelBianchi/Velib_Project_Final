import os
os.environ["SPARK_HOME"] = "/workspaces/Velib_Project_Final/spark-3.2.3-bin-hadoop2.7"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /workspaces/Velib_Project_Final/spark-streaming-kafka-0-10-assembly_2.12-3.2.3.jar pyspark-shell'
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as pysqlf
import pyspark.sql.types as pysqlt

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3',
    'org.apache.kafka:kafka-clients:3.2.3'
]

# Création de la session Spark
spark = (SparkSession.builder
   .config("spark.jars.packages", ",".join(packages))
   .config("spark.sql.repl.eagerEval.enabled", True)
   .getOrCreate()
)

if __name__ == "__main__":
    # Initier spark
    spark = (SparkSession
             .builder
             .appName("velib-project")
             .master("local[1]")
             .config("spark.sql.shuffle.partitions", 1)
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3")
             .getOrCreate()
             )

    # Lire les données temps réel depuis le topic Kafka
    kafka_df = (spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "velib-projet")
                .option("startingOffsets", "earliest")
                .load()
                )

    # Charger les données du fichier CSV des stations
    station_info_df = spark \
        .read \
        .csv("stations_information.csv", header=True)

   
    schema = pysqlt.StructType([
        pysqlt.StructField("stationCode", pysqlt.StringType()),
        pysqlt.StructField("num_bikes_available", pysqlt.IntegerType()),
        pysqlt.StructField("numBikesAvailable", pysqlt.IntegerType()),
        pysqlt.StructField("num_bikes_available_types", pysqlt.ArrayType(pysqlt.MapType(pysqlt.StringType(), pysqlt.IntegerType()))),
        pysqlt.StructField("num_docks_available", pysqlt.IntegerType()),
        pysqlt.StructField("numDocksAvailable", pysqlt.IntegerType()),
        pysqlt.StructField("is_installed", pysqlt.IntegerType()),
        pysqlt.StructField("is_returning", pysqlt.IntegerType()),
        pysqlt.StructField("is_renting", pysqlt.IntegerType()),
        pysqlt.StructField("last_reported", pysqlt.TimestampType())
    ])

    kafka_df = (kafka_df
                .select(pysqlf.from_json(pysqlf.col("value").cast("string"), schema).alias("value"))
                .select("value.*")  # Sélectionner les colonnes structurées
                )

    # Joindre les données de Kafka avec les données des stations
    joined_df = kafka_df.join(station_info_df, kafka_df["stationCode"] == station_info_df["stationCode"], "inner")

    # Calculer les indicateurs par code postal
    indicators_df = (joined_df
                     .groupBy("postcode")
                     .agg(pysqlf.sum("num_bikes_available").alias("total_bikes"),
                          pysqlf.sum("num_bikes_available").alias("total_mechanical_bikes"),
                          pysqlf.sum("numBikesAvailable").alias("total_electric_bikes"))
                     .withColumn("timestamp", pysqlf.current_timestamp())
                     .select("timestamp", "postcode", "total_bikes", "total_mechanical_bikes", "total_electric_bikes")
                     )
    
    # Écrire les infos dans le topic Kafka velib-projet-final-data et afficher dans la console
    query_for_stream_2 = (indicators_df
                        .selectExpr("CAST(timestamp AS STRING) AS key", "to_json(struct(*)) AS value")
                        .writeStream
                        .format("kafka")
                        .option("kafka.bootstrap.servers", "localhost:9092")
                        .option("topic", "velib-projet-final-data")
                        .outputMode("update")
                        .option("checkpointLocation", "/workspaces/Velib_Project_Final")  # Spécifiez l'emplacement du checkpoint ici
                        .start()
                        )

    # Écrire les infos dans la console
    query_console = (indicators_df
                    .writeStream
                    .outputMode("update")
                    .format("console")
                    .start()
                    )

    #terminaison de la requête kafka
    query_for_stream_2.awaitTermination()
    #Terminaison de la requête console
    query_console.awaitTermination()
