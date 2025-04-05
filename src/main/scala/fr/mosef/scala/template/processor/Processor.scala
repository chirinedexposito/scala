package fr.mosef.scala.template.processor

import org.apache.spark.sql.DataFrame

trait Processor {

  // Total des montants par client
  def totalMontantParClient(inputDF: DataFrame): DataFrame

  // Premier crédit par client
  def firstCreditDate(dataFrame: DataFrame): DataFrame

}
