package fr.mosef.scala.template.reader
import org.apache.spark.sql.DataFrame

trait Reader {

  // Lecture générique avec format et options personnalisées (ex: csv, parquet, json...)
  def read(format: String, options: Map[String, String], path: String): DataFrame

  // Lecture CSV (avec paramètres chargés depuis le fichier .properties)
  def read(path: String): DataFrame

  // Lecture d’un fichier Parquet simple
  def readParquet(path: String): DataFrame

  // Lecture d’une table stockée dans un répertoire Parquet partitionné
  def readTable(tableName: String, location: String): DataFrame

  // Retourne un DataFrame vide pour les tests unitaires
  def read(): DataFrame
}
