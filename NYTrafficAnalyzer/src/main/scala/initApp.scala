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
   val fhvTaxiPoller = new FHVTaxiPoller(sc)
    println("Polling for Data..............................................")
    val yellow_rdd1 = yellowTaxiPoller.poll0914
    val yellow_rdd2 = yellowTaxiPoller.poll1516
    val yellow_rdd3 = yellowTaxiPoller.poll1718
    val yellow_rdd4 = yellowTaxiPoller.poll19
    val green_rdd1 = greenTaxiPoller.poll1314
    val green_rdd2 = greenTaxiPoller.poll1516
    val green_rdd3 = greenTaxiPoller.poll1718
    val green_rdd4 = greenTaxiPoller.poll19

    val fhv_rdd1 = fhvTaxiPoller.poll15
    val fhv_rdd2 = fhvTaxiPoller.poll16
    val fhv_rdd3 = fhvTaxiPoller.poll17
    val fhv_rdd4 = fhvTaxiPoller.poll18
    val fhv_rdd5 = fhvTaxiPoller.poll19
    val licenseMappingsRDD = fhvTaxiPoller.pollTaxiMapping
    val weather_rdd = weatherPoller.poll
 
    println("Cleaning Data.................................................")

    val yellow_clean = new YellowTaxiCleaner
    val green_clean = new GreenTaxiCleaner
    val weather_clean = new WeatherCleaner
    val Fhv_clean = new FHVTaxiCleaner
    val cleanYellowRDD=yellow_clean.clean(yellow_rdd1,yellow_rdd2,yellow_rdd3,yellow_rdd4)
    //cleanYellowRDD.map(line=>line(1)).distinct().collect().foreach(println)
    val cleanGreenRDD=green_clean.clean(green_rdd1,green_rdd2,green_rdd3,green_rdd4)
    val cleanWeatherRDD = weather_clean.clean(weather_rdd)
    val cleanFhvRDD=Fhv_clean.clean(fhv_rdd1,fhv_rdd2,fhv_rdd3,fhv_rdd4,fhv_rdd5)

    println("Analyzing Data................................................")
    //val countAnalysis = new CountAnalyzer(sqlContext,cleanFhvRDD,licenseMappingsRDD)
    //countAnalysis.createSQLTable(cleanYellowRDD,cleanGreenRDD)
    //countAnalysis.createMarketShareSQLTable

    //val greenAnalysis = new GreenTaxiAnalyzer
    //greenAnalysis.createTotalYearCount(cleanGreenRDD)
    //val weatherAnalysis = new WeatherAnalyzer
    val priceAnalysis = new PriceAnalyzer(sqlContext)
    val priceDF=priceAnalysis.calculateTipSpeed(cleanYellowRDD)
    priceDF.persist()
    val linear_regressor = new Regressor(sqlContext,sc)
    linear_regressor.trainAndTest(priceDF)
    linear_regressor.trainAndTestForest(priceDF)
    //weatherAnalysis.generateYellowTaxiWeatherRainCount(cleanGreenRDD,cleanWeatherRDD)
    //weatherAnalysis.generateYellowTaxiWeatherRainAverage(cleanYellowRDD,cleanWeatherRDD)
    //weatherAnalysis.generateYellowTaxiWeatherSnowAverage(cleanYellowRDD,cleanWeatherRDD)
    //priceAnalysis.createTimeCountSQLTable(cleanYellowRDD,cleanGreenRDD)
    //priceAnalysis.createTimePriceSQLTable(cleanYellowRDD,cleanGreenRDD)
  }
}