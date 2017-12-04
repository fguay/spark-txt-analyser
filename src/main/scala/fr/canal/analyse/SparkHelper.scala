package fr.canal.analyse

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkHelper {


  def sparkConf(appName:String) : SparkConf = {
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName(appName)
    conf
  }

  def sparkLocalContext(appName:String) : SparkContext = {
    new SparkContext(sparkConf(appName))
  }

  def sparkSession(appName:String) : SparkSession = {
    SparkSession
      .builder()
      .config(sparkConf(appName))
      .getOrCreate()
  }

  def sparkSession(conf: SparkConf) : SparkSession = {
      SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
  }

}
