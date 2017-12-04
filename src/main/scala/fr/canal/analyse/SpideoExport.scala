package fr.canal.analyse

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


object SpideoExport extends App {
    val s = new SpideoExport()
    s.read()
  }

class SpideoExport extends Serializable {

  val logger = LoggerFactory.getLogger(classOf[StatsExportG9])

  def read(): Unit = {
    val sc = SparkSession
      .builder()
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test.User")
      .master("local")
      .appName("SpideoExport")
      .getOrCreate()

    import sc.implicits._

    val readConfigUser = ReadConfig(Map("collection" -> "User"), Some(ReadConfig(sc)))
    val readConfigInter = ReadConfig(Map("collection" -> "Interaction"), Some(ReadConfig(sc)))
    val rddUser = MongoSpark.load(sc, readConfigUser).select($"_id"("oid") as "_id", $"userInfos"("CANAL")("id") as ("canalId") ).cache()
    val rddUserWithProfile = rddUser.filter(r => r.getString(1).startsWith("pf:")).cache()
    val rddUserWithoutProfile = rddUser.filter(r => !r.getString(1).startsWith("pf:")).cache()

    //val rddInterract = MongoSpark.load(sc, readConfigInter).cache()

    //val interact = rddInterract.select("userId", "videoId","name", "value" ).cache()
    logger.info("******** RESULTAT *********** ")
    logger.info("rddUser : " + rddUser.count())
    logger.info("rddUserWithProfile : " +rddUserWithProfile.count())
    logger.info("rddUserWithoutProfile : " +rddUserWithoutProfile.count())


    //val join = interact.join(rddUser, interact("userId") === rddUser("_id")).select("userId", "canalId", "videoId","name", "value").cache
    //join.printSchema()
    //join.show()


  }

}