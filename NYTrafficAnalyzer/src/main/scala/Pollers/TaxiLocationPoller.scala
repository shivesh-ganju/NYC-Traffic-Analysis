package Pollers
import org.apache.spark.SparkContext
class TaxiLocationPoller(val sc:SparkContext) {
    def poll : org.apache.spark.rdd.RDD[String] = {
    val file = "taxiLookup.csv"
    val TaxiLocationRDD = sc.textFile(file)
    TaxiLocationRDD
  }
}