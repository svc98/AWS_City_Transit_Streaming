import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql.functions import from_json, col

from jobs.config.config import configuration


if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("City Transit Streaming")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                                            "org.apache.hadoop:hadoop-aws:3.4.0,"
                                            "com.amazonaws:aws-java-sdk:1.11.469")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))
             .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))
             .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
             .getOrCreate()
             )
    logging.info("Spark connection created successfully!")

    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceID", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ])

    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceID", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])

    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceID", StringType(), True),
        StructField("cameraID", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True)
    ])

    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceID", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", StringType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True)
    ])

    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceID", StringType(), True),
        StructField("incidentID", StringType(), True),
        StructField("type", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("status", StringType(), True)
    ])


    # Streaming
    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col('value'), schema).alias('data')).select("data.*")
                .withWatermark('timestamp', '5 minutes'))

    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .trigger(processingTime='5 seconds')
                .start())


    vehicleDF = read_kafka_topic('vehicle_data', schema=vehicleSchema)
    gpsDF = read_kafka_topic('gps_data', schema=gpsSchema)
    trafficCameraDF = read_kafka_topic('traffic_camera_data', schema=trafficSchema)
    weatherDF = read_kafka_topic('weather_data', schema=weatherSchema)
    emergencyDF = read_kafka_topic('emergency_data', schema=emergencySchema)

    query1 = streamWriter(vehicleDF, 's3a://spark-streaming-bucket/checkpoints/vehicle_data', 's3a://spark-streaming-bucket/data/vehicle_data/')
    query2 = streamWriter(gpsDF, 's3a://spark-streaming-bucket/checkpoints/gps_data', 's3a://spark-streaming-bucket/data/gps_data/')
    query3 = streamWriter(trafficCameraDF, 's3a://spark-streaming-bucket/checkpoints/traffic_camera_data', 's3a://spark-streaming-bucket/data/traffic_camera_data/')
    query4 = streamWriter(weatherDF, 's3a://spark-streaming-bucket/checkpoints/weather_data', 's3a://spark-streaming-bucket/data/weather_data/')
    query5 = streamWriter(emergencyDF, 's3a://spark-streaming-bucket/checkpoints/emergency_data', 's3a://spark-streaming-bucket/data/emergency_data/')

    query5.awaitTermination()