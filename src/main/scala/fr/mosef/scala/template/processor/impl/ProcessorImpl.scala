package fr.mosef.scala.template.processor.impl

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.{col, lit, sum, min, avg}

class ProcessorImpl() extends Processor {

  // Total des montants par client
  def totalMontantParClient(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("client_id")
      .agg(sum("montant").alias("total_montant"))
  }


  // Premier crédit par client
  def firstCreditDate(df: DataFrame): DataFrame = {
    df.groupBy("client_id")
      .agg(min("date_ouverture").alias("premier_credit"))
  }

}
