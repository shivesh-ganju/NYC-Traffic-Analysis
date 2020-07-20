package Preprocessors

class TaxiLocationProcessor {
// Modifies the location id to zone mapping in a convenient tuple format
  def clean(baseMap :org.apache.spark.rdd.RDD[String]) : org.apache.spark.rdd.RDD[(String, (String,String))]= {
    val filteredRDD = baseMap.map(_.split(','))
                             .filter(_.length==4)
                             .map(line=>(line(0),(line(1),line(2))))
                
    filteredRDD
  }
}