package Preprocessors
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
// Profiler for FHV
class FHVTaxiProfiler(val sc:org.apache.spark.SparkContext) {
// This method takes in all the raw FHV data from 2015 to 2019, 
// post which reads the cleaned and merged FHV Data and outputs the
// difference of lines count.
	def profile ={
		val rdd1 = sc.textFile("/user/rs6980/project/folder_fhv/*.csv")
		val rdd2 = sc.textFile("/user/rs6980/project/folder_fhvhv19/2019*.csv")
		val a = rdd1.count + rdd2.count
		println("Count on unclean dataset is " + a)
		val rdd = sc.textFile("/user/rs6980/project/finalMergedDataFHV*")
		val b = rdd.count
		println("Count on unclean dataset is " + b)
		
		println("Difference in rows = " + (a-b).toString)
	}
  
}