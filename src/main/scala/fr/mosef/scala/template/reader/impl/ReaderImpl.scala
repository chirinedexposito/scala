package fr.mosef.scala.template.reader.impl

import java.io.FileInputStream
import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.reader.Reader

class ReaderImpl(sparkSession: SparkSession, propertiesFilePath: String) extends Reader {

  val properties: Properties = new Properties()
  properties.load(new FileInputStream(propertiesFilePath))

  def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession
      .read
      .options(options)
      .format(format)
      .load(path)
  }

  def read(path: String): DataFrame = {
    sparkSession
      .read
      .option("sep", properties.getProperty("read_separator", ","))
      .option("inferSchema", properties.getProperty("schema", "true"))
      .option("header", properties.getProperty("read_header", "true"))
      .format(properties.getProperty("read_format_csv", "csv"))
      .load(path)
  }

  def readParquet(path: String): DataFrame = {
    sparkSession
      .read
      .format(properties.getProperty("read_format_parquet", "parquet"))
      .load(path)
  }

  def readTable(tableName: String, location: String): DataFrame = {
    sparkSession
      .read
      .format("parquet") // ou hive si table Hive
      .option("basePath", location)
      .load(location + "/" + tableName)
  }

  def read(): DataFrame = {
    sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation'")
  }
}
