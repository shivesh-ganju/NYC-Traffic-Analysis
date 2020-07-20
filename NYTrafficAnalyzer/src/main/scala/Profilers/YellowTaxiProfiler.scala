import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

class YellowTaxiProfiler(val sc:org.apache.spark.SparkContext) {
// Prints the difference in dataset size of clean and unclean data
	def profile ={
		val rdd1 = sc.textFile("project/yellowTaxi/*")
		val a = rdd1.count
		println("Count on unclean dataset is " + a)
		val rdd = sc.textFile("project/clean_yellow_data/*")
		val b = rdd.count
		println("Count on unclean dataset is " + b)
		//1544989722
		println("Difference in rows = " + (a-b).toString)
	}
  
}