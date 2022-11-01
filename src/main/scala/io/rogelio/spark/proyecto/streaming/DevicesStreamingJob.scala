package io.rogelio.spark.proyecto.streaming

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

// Se añade el implicito context para el futuro
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

;

object DevicesStreamingJob extends StreamingJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Final Project ")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val jsonSchema = StructType(Seq(
      StructField("bytes", LongType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false),
      StructField("app", StringType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false)
    ))
    dataFrame
      .select(from_json($"value".cast(StringType), jsonSchema).as("json"))
      .select($"json.*")
  }

  override def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichDeviceWithMetadata(deviceDF: DataFrame, userMetadataDF: DataFrame): DataFrame = {
    deviceDF.as("device")
      .join(
        userMetadataDF.as("user"),
        $"device.id" === $"user.id"
      )
      .drop($"user.id")

  }

  override def computeBytesReceivedByAntenna(dataFrame: DataFrame): DataFrame = {
    // bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT);
    dataFrame
      .select($"antenna_id", $"timestamp", $"bytes")
      .withWatermark("timestamp", "5 seconds") // 1 minute
      .groupBy($"antenna_id", window($"timestamp","5 minutes")) //5 minutes
      .agg(
        sum($"bytes").as("value")
      )
      .select(
        $"window.start".as("timestamp"),
        $"antenna_id".as("id"),
        $"value")
      .withColumn("type", lit("total_bytes_by_antenna"))
  }

  override def computeBytesTransmitedByUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"id", $"timestamp", $"bytes")
      .withWatermark("timestamp", "5 seconds") // 1 minute
      .groupBy($"id", window($"timestamp","5 minutes"))
      .agg(
        sum($"bytes").as("value")
      )
      .select(
        $"window.start".as("timestamp"),
        $"id",
        $"value"
      )
      .withColumn("type", lit("total_bytes_transmited_by_user"))
  }

  override def computeBytesTransmitedByApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"app", $"timestamp",$"bytes")
      .withWatermark("timestamp", "5 seconds") // 1 minute
      .groupBy($"app", window($"timestamp", "5 minutes"))
      .agg(
        sum($"bytes").as("value")
      )
      .select(
        $"window.start".as("timestamp"),
        $"app".as("id"),
        $"value"
      )
      .withColumn("type", lit("total_bytes_transmited_by_app"))
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
      dataFrame
        .writeStream
        .foreachBatch {
          (batch: DataFrame, _: Long) => {
            batch
              .write
              .mode(SaveMode.Append)
              .format("jdbc")
              .option("url", jdbcURI)
              .option("dbtable", jdbcTable)
              .option("user", user)
              .option("password", password)
              .save()
          }
        }
        .start()
        .awaitTermination()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    dataFrame
      .select(
        $"bytes", $"timestamp", $"app", $"id", $"antenna_id",
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour")
      )
      .writeStream
      .format("parquet")
      .option("path", s"$storageRootPath/data")
      .option("checkpointLocation", s"$storageRootPath/checkpoint")
      .partitionBy("year", "month", "day", "hour")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    /*
    * Con estas variables se puede ejecutar el proceso una vez construido
    * val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")
    * */

    //run(args)
    // 1 Obtain stream
    val kafkaDF = readFromKafka("34.125.190.236:9092", "devices")
    // 2. Parse to json format
    val parserDF = parserJsonData(kafkaDF)

    // 6. save to storage
    val storageFuture = writeToStorage(parserDF, "/tmp/device_parquet/")

    // 3. Prepare connection to metadata
    val userMetadataDF = readUserMetadata(
      "jdbc:postgresql://34.138.76.194:5432/big_data_processing",
      "user_metadata",
      "postgres",
      "1Q2W3E-1q2w3e"
    )
    // 4 Enrich data using join
    val enrichDF = enrichDeviceWithMetadata(parserDF, userMetadataDF)

    // 6.alternative: save to storage
    // val storageFuture = writeToStorage(enrichDF, "/tmp/device_parquet/")
    //// 5 Agregaciones, agrupamientos, sql

    val totalBytesReceivedByAntenna = computeBytesReceivedByAntenna(enrichDF)
    val jdbcFutureComputedByAntenna = writeToJdbc(totalBytesReceivedByAntenna,
      "jdbc:postgresql://34.138.76.194:5432/big_data_processing",
      "bytes",
      "postgres",
      "1Q2W3E-1q2w3e")

    val totalBytesTransmitedByUser = computeBytesTransmitedByUser(enrichDF)
    val jdbcFutureComputedByUser = writeToJdbc(totalBytesTransmitedByUser,
      "jdbc:postgresql://34.138.76.194:5432/big_data_processing",
      "bytes",
      "postgres",
      "1Q2W3E-1q2w3e")

    val totalBytesTransmitedByApp = computeBytesTransmitedByApp(enrichDF)
    val jdbcFutureComputedByApp = writeToJdbc(totalBytesTransmitedByApp,
      "jdbc:postgresql://34.138.76.194:5432/big_data_processing",
      "bytes",
      "postgres",
      "1Q2W3E-1q2w3e")

    Await.result(
      Future.sequence(Seq(
        storageFuture,
        jdbcFutureComputedByAntenna,
        jdbcFutureComputedByUser,
        jdbcFutureComputedByApp)), Duration.Inf
    )




    // 1. kafkaDF
    // 2. parserDF
    // 3. metadataDF
    // 4. enrichDF
    // 5.     countByLocation
    // 6.1

/*
    enrichDF
      .writeStream
      .format("console")
      .start()
      .awaitTermination()
    */

  }

  override def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame = ???
}
