package Pollers
import org.apache.spark.SparkContext
class TrafficPoller(val sc:SparkContext) {
    def poll : org.apache.spark.rdd.RDD[String] = {
      val file = "Traffic_Volume_Counts__2014-2018_.csv"
      val trafficRDD = sc.textFile(file)
      trafficRDD
  }
}