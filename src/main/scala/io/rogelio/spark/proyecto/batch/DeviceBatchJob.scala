package io.rogelio.spark.proyecto.batch
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame,SaveMode, SparkSession}

import java.time.OffsetDateTime

object DeviceBatchJob extends BatchJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Final Excercise ")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {

    spark
      .read
      .format("parquet")
      .load(s"$storagePath/data")
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
  }

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = ???

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
  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = ???

  override def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame = ???

  override def computeErrorAntennaByModelAndVersion(dataFrame: DataFrame): DataFrame = ???

  override def computePercentStatusByID(dataFrame: DataFrame): DataFrame = ???



  override def computeBytesReceivedByAntenna(dataFrame: DataFrame): DataFrame = {
    // bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT);
    dataFrame
      .select($"antenna_id", $"timestamp", $"bytes")
      .groupBy($"antenna_id", window($"timestamp", "30 seconds")) //5 minutes
      .agg(
        sum($"bytes").as("value")
      )
      .select(
        $"window.start".as("timestamp"),
        $"antenna_id".as("id"),
        $"value")
      .withColumn("type", lit("total_bytes_by_antenna"))
  }

  override def computeBytesPerMailPerUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"email", $"timestamp", $"bytes")
      .groupBy($"email", window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as("value")
      )
      .select(
        $"window.start".as("timestamp"),
        $"email".as("id"),
        $"value"
      )
      .withColumn("type", lit("total_bytes_transmited_by_email"))
  }

  override def computeBytesTransmitedPerApplication(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"app", $"timestamp", $"bytes")
      .groupBy($"app", window($"timestamp", "1 hour"))
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

  override def computeEmailExceedQuota(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"email", $"timestamp", $"bytes", $"name", $"quota")
      .groupBy($"email", window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as("usage"),
        max($"quota").as("quota")
      )
      .select(
        $"window.start".as("timestamp"),
        $"email",
        $"usage",
        $"quota"
      )
      .filter($"usage" > $"quota")
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .format("parquet")
      .partitionBy("year", "month", "day", "hour")
      .mode(SaveMode.Overwrite)
      .save(s"$storageRootPath/historical")
  }

  def main(args: Array[String]): Unit = {

    /*

    * val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, aggJdbcTable, aggJdbcErrorTable, aggJdbcPercentTable, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")
    * */
    val offsetDateTime = OffsetDateTime.parse("2022-10-31T22:00:00Z")
    val parquetDF = readFromStorage("/tmp/device_parquet/", offsetDateTime)
    val userMetadataDF = readUserMetadata(
      "jdbc:postgresql://34.138.76.194:5432/big_data_processing",
      "user_metadata",
      "postgres",
      "1Q2W3E-1q2w3e"
    )
    val deviceUserMetadataDF = enrichDeviceWithMetadata(parquetDF, userMetadataDF).cache()

    val aggBytesReceivedByAntenna = computeBytesReceivedByAntenna(deviceUserMetadataDF)
    val aggBytesPerMailPerUse = computeBytesPerMailPerUser(deviceUserMetadataDF)
    val aggBytesTransmitedPerApplication = computeBytesTransmitedPerApplication(deviceUserMetadataDF)
    val aggEmailExceedQuota = computeEmailExceedQuota(deviceUserMetadataDF)

    writeToJdbc(
      aggBytesReceivedByAntenna,
      "jdbc:postgresql://34.138.76.194:5432/big_data_processing",
      "bytes_hourly",
      "postgres",
      "1Q2W3E-1q2w3e" )
    writeToJdbc(
      aggBytesPerMailPerUse,
      "jdbc:postgresql://34.138.76.194:5432/big_data_processing",
      "bytes_hourly",
      "postgres",
      "1Q2W3E-1q2w3e")
    writeToJdbc(
      aggBytesTransmitedPerApplication,
      "jdbc:postgresql://34.138.76.194:5432/big_data_processing",
      "bytes_hourly",
      "postgres",
      "1Q2W3E-1q2w3e")
    writeToJdbc(
      aggEmailExceedQuota,
      "jdbc:postgresql://34.138.76.194:5432/big_data_processing",
      "user_quota_limit",
      "postgres",
      "1Q2W3E-1q2w3e")
    writeToStorage(parquetDF, "/tmp/device_parquet/")

    spark.close()

  }


}
