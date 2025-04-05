package fr.mosef.scala.template.job

import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.writer.Writer

trait Job {
  val reader: Reader
  val processor: Processor
  val writer: Writer
  val srcPath: String
  val dstPath: String
  val format: String
}
