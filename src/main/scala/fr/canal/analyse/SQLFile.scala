package fr.canal.analyse

/**
 * Everyone's favourite wordcount example.
 */

import org.apache.spark.rdd._
import org.slf4j.{Logger, LoggerFactory}

object SQLFile extends App {
  val s = new SQLFile()
  s.analyse()
}

class SQLFile extends Serializable{

  val logger = LoggerFactory.getLogger(classOf[SQLFile])

  def analyse(): Unit = {

    val sc = SparkHelper.sparkSession("testExportG9")
    import sc.implicits._

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val diffDF = sc.sparkContext.textFile("src/main/resources/output.txt")
      .map(_.split(";"))
      .map(attributes => DiffIds(attributes(0), attributes(1),attributes(2), attributes(3)))
      .toDF()

    diffDF.createOrReplaceTempView("output")

    val ko = sc.sql("SELECT * FROM output WHERE statusEdito='Not Found'")
    val ok = sc.sql("SELECT * FROM output WHERE statusEdito='OK'")

    diffDF.show()


    val sumko = ko.count()
    logger.info("Sum KO : " + sumko + "\n")
    val sumok = ok.count()
    logger.info("Sum OK : " + sumok + "\n")

    sc.stop()
  }
}

case class DiffIds(plmId:String, statusPLM:String, editoId:String, statusEdito:String)
