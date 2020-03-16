package Pollers
import org.apache.spark.SparkContext
class YellowTaxiPoller(val sc:SparkContext) {
  
  def poll : org.apache.spark.rdd.RDD[String] = {
    val file = "yellowTaxi/*"
    val yellowTaxiRDD = sc.textFile(file)
    yellowTaxiRDD
  }
 
  
  
}