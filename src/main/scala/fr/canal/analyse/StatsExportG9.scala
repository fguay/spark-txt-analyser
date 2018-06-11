package fr.canal.analyse

/**
 * Everyone's favourite wordcount example.
 */

import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

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
      .map(attributes => EditoIds(attributes(0), attributes(1), attributes(2), attributes(3)))
      .toDF()

    editoDF.createOrReplaceTempView("edito")

    val brandDF = sc.sparkContext.textFile("/tmp/brand.txt")
      .map(_.split(";"))
      .map(attributes => {
         val latt = attributes.toList
           BrandIds(latt(0), latt.lift(1))
        })
      .toDF()

    brandDF.createOrReplaceTempView("brand")

    val seasonDF = sc.sparkContext.textFile("/tmp/season.txt")
      .map(_.split(";"))
      .map(attributes => {
        val latt = attributes.toList
        SeasonIds(latt(0), latt.lift(1), latt.lift(2))
      })
      .toDF()

    seasonDF.createOrReplaceTempView("season")


    val distinctDiff = diffDF.select("editoId").distinct().cache()
    val distinctEdito = editoDF.select("editoId").distinct().cache()
    val matchs = distinctDiff.intersect(distinctEdito).cache()
    val noEdito = distinctDiff.except(distinctEdito).cache()
    val noDiff = distinctEdito.except(distinctDiff).cache()

    val noEditoAlias = noEdito.withColumnRenamed("editoId","noEditoId")
    val join = noEditoAlias.join(diffDF,noEditoAlias("noEditoId") === diffDF("editoId")).select("editoId","plmId","channelId").cache()


    val sumDiff = diffDF.count()
    val sumEdito = editoDF.count()
    val sumMatch = matchs.count()
    val sumNoEdito = noEdito.count()
    val sumDiffNoEdito = join.count()

    val brand = brandDF.select("brandId")
    val season = seasonDF.select("seasonId")

    val sumBrand = brand.count()
    val sumSeason = season.count()

    val editoBrand = editoDF.select("brandId").filter( r => !r.equals("")).cache()
    val seasonBrand = seasonDF.select("brandId").filter( r => !r.equals("")).cache()
    val editoSeason = editoDF.select("seasonId").filter( r => !r.equals("")).cache()

    val noBrandEdito = editoBrand.except(brand)
    val noSeasonEdito = editoSeason.except(season)
    val noBrandSeason = seasonBrand.except(brand)


    val noBrandEditoDistinct = noBrandEdito.distinct()
    val noSeasonEditoDistinct = noSeasonEdito.distinct()
    val noBrandSeasonDistinct = noBrandSeason.distinct()

    val sumEditoBrand = editoBrand.distinct().count()
    val sumEditoSeason = editoSeason.distinct().count()
    val sumNoBrandEdito = noBrandEditoDistinct.count()
    val sumNoSeasonEdito = noSeasonEditoDistinct.count()
    val sumNoBrandSeason = noBrandSeasonDistinct.count()

    join.coalesce(1).write.mode(SaveMode.Overwrite).csv("/tmp/result-data")
    noDiff.coalesce(1).write.mode(SaveMode.Overwrite).csv("/tmp/result-nodiff")
    noEditoAlias.coalesce(1).write.mode(SaveMode.Overwrite).csv("/tmp/result-nodiff-plm")

    noBrandEditoDistinct.coalesce(1).write.mode(SaveMode.Overwrite).csv("/tmp/result-noBrandEdito")
    noSeasonEditoDistinct.coalesce(1).write.mode(SaveMode.Overwrite).csv("/tmp/result-noSeasonEdito")
    noBrandSeasonDistinct.coalesce(1).write.mode(SaveMode.Overwrite).csv("/tmp/result-noBrandSeason")

    import org.apache.hadoop.fs._

    val fs = FileSystem.get(sc.sparkContext.hadoopConfiguration)

    val file = fs.globStatus(new Path("/tmp/result-data/part*"))(0).getPath().getName()
    fs.rename(new Path("/tmp/result-data/" + file), new Path("/tmp/result-analyse.csv"))
    val file2 = fs.globStatus(new Path("/tmp/result-nodiff/part*"))(0).getPath().getName()
    fs.rename(new Path("/tmp/result-nodiff/" + file2), new Path("/tmp/result-nodiff-analyse.csv"))
    val file3 = fs.globStatus(new Path("/tmp/result-nodiff-plm/part*"))(0).getPath().getName()
    fs.rename(new Path("/tmp/result-nodiff-plm/" + file3), new Path("/tmp/result-nodiff-plm-analyse.csv"))

    val file4 = fs.globStatus(new Path("/tmp/result-noBrandEdito/part*"))(0).getPath().getName()
    fs.rename(new Path("/tmp/result-noBrandEdito/" + file4), new Path("/tmp/result-noBrandEdito.csv"))
    val file5 = fs.globStatus(new Path("/tmp/result-noSeasonEdito/part*"))(0).getPath().getName()
    fs.rename(new Path("/tmp/result-noSeasonEdito/" + file5), new Path("/tmp/result-noSeasonEdito.csv"))
    val file6 = fs.globStatus(new Path("/tmp/result-noBrandSeason/part*"))(0).getPath().getName()
    fs.rename(new Path("/tmp/result-noBrandSeason/" + file6), new Path("/tmp/result-noBrandSeason.csv"))


    sc.stop()

    logger.info("*******************")
    logger.info("Sum edito exported : " + sumEdito)
    logger.info("Sum broadcast exported: " + sumDiff )
    logger.info("Sum match broadcast <-> edito : " + sumMatch)
    logger.info("Sum edito ref in broadcast not in edito : " + sumNoEdito)
    logger.info("Sum broadcast without edito : " + sumDiffNoEdito)
    logger.info("*******************")
    logger.info("Sum brand exported : " + sumBrand)
    logger.info("Sum season exported : " + sumSeason)
    logger.info("Sum brand ref in edito export : " + sumEditoBrand)
    logger.info("Sum season ref in edito export : " + sumEditoSeason)
    logger.info("Sum brand in edito not in brand (noBrandEdito): " + sumNoBrandEdito)
    logger.info("Sum season in edito not in season (noSeasonEdito): " + sumNoSeasonEdito)
    logger.info("Sum brand in season not in brand (noBrandSeason): " + sumNoBrandSeason)
    logger.info("*******************")



  }
}

case class BroadCastIds (plmId:String, editoId:String, channelId:String)
case class EditoIds(editoId:String, seasonId:String, brandId:String, channelODId:String)
case class BrandIds(brandId:String, channelODId:Option[String])
case class SeasonIds(seasonId:String, brandId:Option[String], channelODId:Option[String])


