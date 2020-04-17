package Analyzer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
class PriceAnalyzer(val sqlContext:SQLContext){

	def pickupsurgeCountAnalyzer(taxiRDD:org.apache.spark.rdd.RDD[Seq[String]])={
		val tempRDD = taxiRDD.map(line=>(line(9),1))
							 .reduceByKey((v1,v2)=>v1+v2)
		tempRDD
	}

	def pickupsurgeAvgAnalyzer(taxiRDD:org.apache.spark.rdd.RDD[Seq[String]])={
		val tempRDD = taxiRDD.map(line=>(line(9),(line(7).toFloat,1)))
							 .reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2))
							 .map(line=>(line._1,line._2._1/line._2._2))
		tempRDD
	}

	def createTimeCountSQLTable(yellowtaxi:org.apache.spark.rdd.RDD[Seq[String]],
					   			greentaxi:org.apache.spark.rdd.RDD[Seq[String]])={
		import sqlContext._
		import sqlContext.implicits._
		val yellowtaxiRDD = pickupsurgeCountAnalyzer(yellowtaxi)
		val greentaxiRDD = pickupsurgeCountAnalyzer(greentaxi)
		val finalRDD = yellowtaxiRDD.join(greentaxiRDD);
		val transitRDD = finalRDD.map(line => Row(line._1,line._2._1,line._2._2))
		val schema = StructType(
						List(
							StructField("Time_range",StringType,true),
							StructField("YellowTaxiCount",IntegerType,true),
							StructField("GreenTaxiCount",IntegerType,true)
						)
					)
		val countDF = sqlContext.createDataFrame(transitRDD,schema)
		val finalDF = countDF.sort($"Time_range")
		finalDF.write.saveAsTable("sg6148.Analysis2_1")
	}

	def createTimePriceSQLTable(yellowtaxi:org.apache.spark.rdd.RDD[Seq[String]],
					   			greentaxi:org.apache.spark.rdd.RDD[Seq[String]])={
		import sqlContext._
		import sqlContext.implicits._
		val yellowtaxiRDD = pickupsurgeAvgAnalyzer(yellowtaxi)
		val greentaxiRDD = pickupsurgeAvgAnalyzer(greentaxi)
		val finalRDD = yellowtaxiRDD.join(greentaxiRDD).sortByKey()	;
		val transitRDD = finalRDD.map(line => Row(line._1,line._2._1,line._2._2))
		val schema = StructType(
						List(
							StructField("Time_range",StringType,true),
							StructField("YellowTaxiPrice",FloatType,true),
							StructField("GreenTaxiPrice",FloatType,true)
						)
					)
		val countDF = sqlContext.createDataFrame(transitRDD,schema)
		val finalDF = countDF.sort($"Time_range")
		finalDF.write.saveAsTable("sg6148.Analysis2_2")
	}

}