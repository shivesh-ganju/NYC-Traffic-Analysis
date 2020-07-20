package Analyzer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.io.Serializable
/*
* This class has functions which help in finding out insights related to the taxi fare
* and then stores it into the HDFS
*/
class PriceAnalyzer(val sqlContext:SQLContext) extends Serializable{
//This function is responsible for finding out the number of pickups per hour range. The time has been bucketed
// and then the counts are aggregrated
	def pickupsurgeCountAnalyzer(taxiRDD:org.apache.spark.rdd.RDD[Seq[String]])={
		val tempRDD = taxiRDD.map(line=>(line(9),1))
							 .reduceByKey((v1,v2)=>v1+v2)
		tempRDD
	}
// This function helps in calculating the average fare per hour bucket
	def pickupsurgeAvgAnalyzer(taxiRDD:org.apache.spark.rdd.RDD[Seq[String]])={
		val tempRDD = taxiRDD.map(line=>(line(9),(line(7).toFloat,1)))
							 .reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2))
							 .map(line=>(line._1,line._2._1/line._2._2))
		tempRDD
	}
// This function creates a Hive table where the table contains the bucketed hours and the number of pickups for 
// yellow and green taxi per hour bucket
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
		finalDF.write.saveAsTable("sg6148.Analysis2_1_Final")
	}
// This function creates a Hive table where the table contains the bucketed hours and the average fare price for 
// yellow and green taxi per hour bucket
	def createTimePriceSQLTable(yellowtaxi:org.apache.spark.rdd.RDD[Seq[String]],
					   			greentaxi:org.apache.spark.rdd.RDD[Seq[String]])={
		import sqlContext._
		import sqlContext.implicits._
		val yellowtaxiRDD = pickupsurgeAvgAnalyzer(yellowtaxi)
		val greentaxiRDD = pickupsurgeAvgAnalyzer(greentaxi)
		val finalRDD = yellowtaxiRDD.join(greentaxiRDD)
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
		finalDF.write.saveAsTable("sg6148.Analysis2_2_Final")
	}
// This function first calculates the speed of the taxis as distance divided by duration. It then calculates the tip% as tip amount
// divided by total amount and then bucketizes the speed to find out the average tip% given per speed bucket.
	def calculateTipSpeed(taxiRDD:org.apache.spark.rdd.RDD[Seq[String]])={
		import sqlContext._
		import sqlContext.implicits._
		val testing=taxiRDD.filter(line=>line(13).toInt>=0)
		val speedRDD = taxiRDD.filter(line=>line(1).toFloat<50&&line(1).toFloat>0&&line(13).toInt>=0)
		 					   .filter(_(12).toFloat>0)
		 					   .filter(line=>Math.round(line(1).toFloat/line(12).toFloat)<=50)
		 					   .filter(_(6).toFloat>0)
		 					   .filter(line=>(line(6).toFloat)*100/(line(7).toFloat)<50)
		 					   .filter(line=>line(14)=="1")
		 					   .map(line=>(bucketize(line(1).toFloat/line(12).toFloat),(((line(6).toFloat)*100)/(line(7).toFloat),1)))
		 					   .reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2))
		 					   .map(line=>(line._1,line._2._1/line._2._2)).sortByKey()
		val schema = StructType(
				List(
					StructField("Speed",FloatType,true),
					StructField("label",FloatType,true)
					)
				)
		val finalRDD = speedRDD.map(line=>Row(line._1,line._2))
		val speedDF = sqlContext.createDataFrame(finalRDD,schema)
		speedDF.write.saveAsTable("sg6148.Analysis2_3_Final")
		speedDF
	}
// This function bucketizes the speed in buckets of 0.5 miles per hour
	def bucketize(num:Float)={
		var frac:Float = num-num.toInt
		if(frac<0.5){
			frac=(0.0).toFloat
		}else{
			frac=(0.5).toFloat
		}
		var result:Float = num.toInt+frac
		result
	}

}