import org.apache.spark.SparkContext
import Gateway._ 
import org.apache.spark.SparkConf
import Pollers._
object initApp {
   def main(args: Array[String]){
//    val conf = new SparkConf().setAppName("test")
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")
//    val yellowTaxiPoller = new YellowTaxiPoller(sc)
//    val greenTaxiPoller = new GreenTaxipoller(sc)
//    val fhvTaxiPoller = new FHVPoller(sc)
//    val yellowRDD = yellowTaxiPoller.poll
//    val greenRDD = greenTaxiPoller.poll
//    val fhvRDD = fhvTaxiPoller.poll
//    println(yellowRDD.count)
//    println(fhvRDD.count)
//    println(greenRDD.count)
//    yellowRDD.take(2).foreach(println)
//    greenRDD.take(2).foreach(println)
//    fhvRDD.take(2).foreach(println)
     val geo = new APIGateway
     geo.test
  }
}