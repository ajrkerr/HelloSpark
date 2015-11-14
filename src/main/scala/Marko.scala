/**
  * Created by adamkerr on 2015-11-13.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.elasticsearch.spark._
import com.typesafe.config.{Config, ConfigFactory}



object Marko {
  def main(args: Array[String]) {
    val conf = ConfigFactory.load()

    val sparkConf = new SparkConf()
      .setAppName(conf.getString("spark.appName"))
      .setMaster(conf.getString("spark.master"))
      .set("es.nodes", conf.getString("elastic.nodes"))
      .set("es.net.http.auth.user", conf.getString("elastic.username"))
      .set("es.net.http.auth.pass", conf.getString("elastic.password"))
      .set("es.port", conf.getString("elastic.port"))
      .set("es.resource", "pm-2013*")

    val sc = new SparkContext(sparkConf)

//    val logFile = "/users/adamkerr/Downloads/Branch_Information.csv"
//    val logData = sc.textFile(logFile, 2).cache()
//    val numAs = logData.filter(line => line.contains("a")).count()
//    val numBs = logData.filter(line => line.contains("b")).count()

    val RDD = sc.esRDD("pm-2013*")
    val num = RDD.count()
    println("Number of index/types: %s".format(num))

  }
}
