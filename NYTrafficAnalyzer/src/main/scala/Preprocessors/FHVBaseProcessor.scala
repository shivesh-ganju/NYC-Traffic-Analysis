package Preprocessors
import org.apache.spark.SparkContext

class FHVBaseCleaner {
  def clean (baseMap :org.apache.spark.rdd.RDD[String] ) : org.apache.spark.rdd.RDD[(String, String)] = {
    val  filteredRDD = baseMap.map(line => line.split(','))
                       .filter(line=>line.length>0)
                       .filter(_(0).startsWith("B"))
                       .map(line => if(line.length>=4)(line(0),line(line.length-1)) else (line(0),"other"))
    filteredRDD                                   
  }
}