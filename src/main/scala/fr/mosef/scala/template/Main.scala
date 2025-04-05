package fr.mosef.scala.template

import fr.mosef.scala.template.job.Job
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import fr.mosef.scala.template.writer.Writer

import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.SparkConf
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs.FileSystem

object Main extends App with Job {

  // ==== Lecture des arguments ====
  val MASTER_URL = if (args.length > 0) args(0) else "local[*]"
  val SRC_PATH = if (args.length > 1) args(1) else "./src/main/resources/credits.csv"
  val DST_PATH = if (args.length > 2) args(2) else "./Outputs/df_preprocessed_csv"
  val DST_PATH_PARQUET = if (args.length > 3) args(3) else "./Outputs/df_preprocessed_parquet"
  val TABLE_PATH = if (args.length > 4) args(4) else "./Outputs/df_preprocessed_hive"
  val PROPERTIES_PATH = if (args.length > 5) args(5) else "./src/main/resources/configuration.properties"

  // ==== Spark session ====
  val conf = new SparkConf().set("spark.driver.memory", "64M")
  val spark = SparkSession.builder()
    .appName("Scala Template")
    .master(MASTER_URL)
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.hadoopConfiguration
    .setClass("fs.file.impl", classOf[BareLocalFileSystem], classOf[FileSystem])

  // ==== Initialisation ====
  override val reader = new ReaderImpl(spark, PROPERTIES_PATH)
  override val processor = new ProcessorImpl()
  override val writer = new Writer(PROPERTIES_PATH)

  override val srcPath: String = SRC_PATH
  override val dstPath: String = DST_PATH
  override val format: String = "csv"

  // ==== Lecture CSV ====
  val inputDF: DataFrame = reader.read(SRC_PATH)
  inputDF.printSchema() //
  inputDF.show(5, false) //


  // ==== Traitements ====
  val dfMontant = processor.totalMontantParClient(inputDF)    // client_id + total_montant
  val dfPremier = processor.firstCreditDate(inputDF)          // client_id + premier_credit

  // ==== Fusion des résultats en une seule table ====
  val df_preprocessed = dfMontant
    .join(dfPremier, Seq("client_id"), "left")

  // ==== Affichage de la table finale ====
  println("=== Table finale :")
  df_preprocessed.show(false)

  // ==== Sauvegarde CSV et Parquet ====
  writer.writeCSV(df_preprocessed, DST_PATH)
  writer.writeParquet(df_preprocessed, DST_PATH_PARQUET)

  // ==== Sauvegarde Hive ====
  df_preprocessed.write
    .mode(SaveMode.Overwrite)
    .option("path", TABLE_PATH)
    .saveAsTable("df_preprocessed")

  println("Traitement terminé avec succès !")
  spark.stop()
}