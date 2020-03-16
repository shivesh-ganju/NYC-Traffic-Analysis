package Pollers
import org.apache.spark.SparkContext

class GreenTaxipoller(val sc:SparkContext) {
    
  def poll : org.apache.spark.rdd.RDD[String] = {
    val file = "GreenTaxi/*"
    val GreenTaxiRDD = sc.textFile(file)
    GreenTaxiRDD
  }
}