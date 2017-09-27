package fr.canal.analyse

/**
 * Everyone's favourite wordcount example.
 */

import org.apache.spark.rdd._
import org.slf4j.{Logger, LoggerFactory}

object StatsExportG9 extends App {
  val s = new StatsExportG9()
  s.analyse()
}

class StatsExportG9 extends Serializable{

  val logger = LoggerFactory.getLogger(classOf[StatsExportG9])

  def analyse(): Unit = {

    val sc = SparkHelper.sparkSession("StatsExportG9")
    import sc.implicits._

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val diffDF = sc.sparkContext.textFile("/tmp/broadcast.txt")
      .map(_.split(";"))
      .map(attributes => BroadCastIds(attributes(0), attributes(1),attributes(2)))
      .toDF()

    diffDF.createOrReplaceTempView("diff")

    val editoDF = sc.sparkContext.textFile("/tmp/edito.txt")
      .map(_.split(";"))
      .map(attributes => EditoIds(attributes(0), attributes(1)))
      .toDF()

    editoDF.createOrReplaceTempView("edito")

    val res = diffDF.select("editoId").intersect(editoDF.select("editoId"))

    val sumDiff = diffDF.count()
    val sumEdito = editoDF.count()
    val sumMatch = res.count()

    sc.stop()

    logger.info("*******************")
    logger.info("Sum nb Edito : " + sumEdito)
    logger.info("Sum nb Broadcast : " + sumDiff )
    logger.info("Sum match : " + sumMatch)
    logger.info("*******************")


  }
}

case class BroadCastIds (plmId:String, editoId:String, channelId:String)
case class EditoIds(editoId:String, channelODId:String)

