package Pollers
import org.apache.spark.SparkContext
class WeatherPoller(val sc:SparkContext) {
    def poll : org.apache.spark.rdd.RDD[String] = {
      val file = "project/central_park_weather.csv"
      val weatherRDD = sc.textFile(file)
      weatherRDD
  }
}