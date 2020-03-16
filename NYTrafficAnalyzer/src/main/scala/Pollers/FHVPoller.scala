package Pollers
import org.apache.spark.SparkContext
class FHVPoller(val sc:SparkContext) {
    def poll : org.apache.spark.rdd.RDD[String] = {
    val file = "FHVTaxi/*"
    val FHVTaxiRDD = sc.textFile(file)
    FHVTaxiRDD
  }
}