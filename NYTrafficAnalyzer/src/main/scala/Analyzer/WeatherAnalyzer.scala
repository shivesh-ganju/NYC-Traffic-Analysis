package Analyzer
import org.apache.spark.SparkContext
class WeatherAnalyzer {
  def generateYellowTaxiWeatherRainCount(yellowRDD:org.apache.spark.rdd.RDD[Seq[String]],weatherRDD:org.apache.spark.rdd.RDD[(String, (String, String))]){
  	val rdd = yellowRDD.keyBy(_(8))
  	val combined = rdd.join(weatherRDD)
  	val countRDD = combined.map(line=>(line._2._2._1,1)).reduceByKey((v1,v2)=>v1+v2)
  	countRDD.saveAsTextFile("project/weather_count_green/")
  }
  def generateYellowTaxiWeatherRainAverage(yellowRDD:org.apache.spark.rdd.RDD[Seq[String]],weatherRDD:org.apache.spark.rdd.RDD[(String, (String, String))]){
  	val rdd = yellowRDD.map(line=>(line(8),line(7).toFloat))
  	val combined = rdd.join(weatherRDD)
  	val countRDD = combined.map(line=>(line._2._2._1,(line._2._1,1)))
  				   .reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2))
  				   .map(line=>(line._1,line._2._1/line._2._2))
  	countRDD.saveAsTextFile("project/weather_price_green/")
  }

  def generateYellowTaxiWeatherSnowAverage(yellowRDD:org.apache.spark.rdd.RDD[Seq[String]],weatherRDD:org.apache.spark.rdd.RDD[(String, (String, String))]){
  	val rdd = yellowRDD.map(line=>(line(8),line(7).toFloat))
  	val combined = rdd.join(weatherRDD)
  	val countRDD = combined.map(line=>(line._2._2._2,(line._2._1,1)))
  				   .reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2))
  				   .map(line=>(line._1,line._2._1/line._2._2))
  				   .map(line => line.toString)
  				   .map(line=>line.substring(1,line.length-1))
  	countRDD.saveAsTextFile("project/weather_price_snow_yellow/")
  }
}