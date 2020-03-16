package Pollers
import org.apache.spark.SparkContext
class WeatherPoller(val sc:SparkContext) {
    def poll : org.apache.spark.rdd.RDD[String] = {
      val file = "weather_data.csv"
      val weatherRDD = sc.textFile(file)
      weatherRDD
  }
}