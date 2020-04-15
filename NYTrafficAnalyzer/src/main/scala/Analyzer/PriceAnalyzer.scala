package Analyzer
import org.apache.spark.SparkContext
class PriceAnalyzer{

	def avgPricePerPassengerCount(taxiRDD:org.apache.spark.rdd.RDD[Seq[String]])={
		val tempRDD = taxiRDD.map(line=>(line(1),(line(7).toFloat,1)))
							 .reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2))
							 .map(line=>(line._1,line._2._1/line._2._2))
							 .map(_.toString)
							 .map(line=>line.substring(1,line.length-1))
		tempRDD.saveAsTextFile("project/avgPricePerPassengerCount2/")
	}
}