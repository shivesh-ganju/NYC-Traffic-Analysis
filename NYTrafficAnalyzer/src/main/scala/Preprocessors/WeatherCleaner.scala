package Preprocessors
import java.io.Serializable
// This class is used for cleaning purposes for weather data.
class WeatherCleaner extends Serializable{

// This method takes in raw uncleaned weather dataset and cleans and formats the data.
// Final RDD which is cleaned is returned.
  def clean(weatherData : org.apache.spark.rdd.RDD[String]):org.apache.spark.rdd.RDD[(String, (String, String))]={
      val wd = weatherData.map(_.split(",")).filter(_.length==10)
      val fwd = wd.map(line=>line.map(elem=>if(elem.length==0)"\"0\"" else elem))
      val weatherRDD = fwd.map(line=>line.map(elem=>elem.substring(1,elem.length-1)))
                       .map(line=>List(line(3),line(5),line(6)))   
      val format = weatherRDD.map(modify(_)).map(line=>(line._1,(line._2,line._3)))
      format
  }

// This method takes in semi-cleaned weather dataset and generates a tuple of
// date, precipation level, snow level
  def modify(arr:List[String])={
    val year = arr(0)
    var prec:Float = arr(1).toFloat
    var snow:Float = arr(2).toFloat
    var p=""
    var s=""
    if(prec==0)p="0"
    if(snow==0)s="0"
    if(prec>0&&prec<0.2)p="0-0.2"
    if(snow>0&&snow<2)s="0-2"
    if(prec>=0.2&&prec<0.4)p="0.2-0.4"
    if(snow>=2&&snow<4)s="2-4"
    if(prec>=0.4&&prec<0.6)p="0.4-0.6"
    if(snow>=4&&snow<6)s="4-6"
    if(prec>=0.6 && prec<=0.8)p="0.6-0.8"
    if(snow>=6)s=">6"
    if(prec>=0.8)p=">0.8"
    (year,p,s)
  }

}


