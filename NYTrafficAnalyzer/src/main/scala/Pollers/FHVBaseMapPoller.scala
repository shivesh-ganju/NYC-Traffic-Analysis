package Pollers
import org.apache.spark.SparkContext
class FHVBaseMapPoller(val sc:SparkContext) {
    def poll : org.apache.spark.rdd.RDD[String] = {
      val file = "fhv_base.csv"
      val fhvRDD = sc.textFile(file)
      fhvRDD
  }
}