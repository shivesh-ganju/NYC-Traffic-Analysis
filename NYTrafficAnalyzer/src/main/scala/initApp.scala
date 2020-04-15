import org.apache.spark.SparkContext
import Gateway._ 
import org.apache.spark.SparkConf
import Pollers._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
import Preprocessors._
import Analyzer._
object initApp {
   def main(args: Array[String]){
   val conf = new SparkConf().setAppName("test")
   val sc = new SparkContext(conf)
   sc.setLogLevel("ERROR")
   val sqlContext = new SQLContext(sc)
   val yellowTaxiPoller = new YellowTaxiPoller(sc)
   val greenTaxiPoller = new GreenTaxiPoller(sc)
   val weatherPoller = new WeatherPoller(sc)
    println("Polling for Data..............................................")
    val yellow_rdd1 = yellowTaxiPoller.poll0914
    yellow_rdd1.take(4).foreach(println)
    val yellow_rdd2 = yellowTaxiPoller.poll1516
    yellow_rdd2.take(4).foreach(println)
    val yellow_rdd3 = yellowTaxiPoller.poll1718
    yellow_rdd3.take(4).foreach(println)
    val yellow_rdd4 = yellowTaxiPoller.poll19
    yellow_rdd4.take(4).foreach(println)
    val green_rdd1 = greenTaxiPoller.poll1314
    green_rdd1.take(4).foreach(println)
    val green_rdd2 = greenTaxiPoller.poll1516
    green_rdd2.take(4).foreach(println)
    val green_rdd3 = greenTaxiPoller.poll1718
    green_rdd3.take(4).foreach(println)
    val green_rdd4 = greenTaxiPoller.poll19
    green_rdd4.take(4).foreach(println)
    val weather_rdd = weatherPoller.poll
    val yellow_clean = new YellowTaxiCleaner
    val green_clean = new GreenTaxiCleaner
    val weather_clean = new WeatherCleaner
    println("Cleaning Data.................................................")
    val cleanYellowRDD=yellow_clean.clean(yellow_rdd1,yellow_rdd2,yellow_rdd3,yellow_rdd4)
    cleanYellowRDD.map(line=>line(1)).distinct().collect().foreach(println)
    val cleanGreenRDD=green_clean.clean(green_rdd1,green_rdd2,green_rdd3,green_rdd4)
    val cleanWeatherRDD = weather_clean.clean(weather_rdd)
    println("Analyzing Data................................................")
    val yellowAnalysis = new YellowTaxiAnalyzer
    //yellowAnalysis.createTotalYearCount(cleanYellowRDD)
    val greenAnalysis = new GreenTaxiAnalyzer
    //greenAnalysis.createTotalYearCount(cleanGreenRDD)
    val weatherAnalysis = new WeatherAnalyzer
    val priceAnalysis = new PriceAnalyzer
    //weatherAnalysis.generateYellowTaxiWeatherRainCount(cleanGreenRDD,cleanWeatherRDD)
    //weatherAnalysis.generateYellowTaxiWeatherRainAverage(cleanYellowRDD,cleanWeatherRDD)
    //weatherAnalysis.generateYellowTaxiWeatherSnowAverage(cleanYellowRDD,cleanWeatherRDD)
    priceAnalysis.avgPricePerPassengerCount(cleanYellowRDD)
  }
}