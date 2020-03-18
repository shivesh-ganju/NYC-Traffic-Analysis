package Preprocessors

class WeatherProcessor {
  def clean(weatherData : org.apache.spark.rdd.RDD[String]):org.apache.spark.rdd.RDD[(String, (String, String, String, String, String))]={
     val missingValuesNA=weatherData.map(_.split(",")).filter(_.length>0).map(line => line.map(value=>if(value=="") "NA" else value))
     val filtered_missing_input = missingValuesNA.map(line => (line(3),(line(4),line(5),line(6),line(7),line(8))))
     var sum1=0.0
     var avg1=0.0
     val avgs = filtered_missing_input.collect
     for(tuple <- avgs;if(tuple._2._1!="NA")){
       sum1+=tuple._2._1.toDouble
     }
     avg1 = sum1/avgs.length
     val weatherRDD=missingValuesNA.map(line=>line.map(value=>if(value=="NA")avg1.toString else value)).map(line => (line(3),(line(4),line(5),line(6),line(7),line(8))))
     weatherRDD
  }
}