package Analyzer
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
class YellowTaxiAnalyzer{

	def createTotalYearCount(yellowRDD:org.apache.spark.rdd.RDD[Seq[String]])={

		val tempRDD = yellowRDD.map(line => (line(8).substring(0,7),1))
		val countRDD = tempRDD.reduceByKey((v1,v2)=>v1+v2)
		countRDD.persist()
		val formatCount = countRDD.sortByKey()
		val result=formatCount.map(line=>line.toString).map(line=>line.substring(1,line.length-1))
		result.saveAsTextFile("project/count_yellow2")
	}
}