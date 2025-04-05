package fr.mosef.scala.template.writer

import java.util.Properties
import java.io.FileInputStream
import org.apache.spark.sql.DataFrame

class Writer(propertiesFilePath: String) {

  val properties: Properties = new Properties()
  properties.load(new FileInputStream(propertiesFilePath))

  def writeCSV(df: DataFrame, path: String, mode: String = "overwrite"): Unit = {
    df.write
      .option("header", properties.getProperty("write_header")) // Ex: true
      .option("sep", properties.getProperty("write_separator")) // Ex: ;
      .mode(mode)
      .csv(path)
  }

  def writeParquet(df: DataFrame, path: String, mode: String = "overwrite"): Unit = {
    df.write
      .mode(mode)
      .parquet(path)
  }

  def writeTable(df: DataFrame, tableName: String, tablePath: String, mode: String = "overwrite"): Unit = {
    df.write
      .mode(mode)
      .option("path", tablePath)
      .saveAsTable(tableName)
  }
}
