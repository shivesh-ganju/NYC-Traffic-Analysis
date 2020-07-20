package Analyzer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.io.Serializable
/*
* This class has functions which helps in calculating the dataset used for drawing the ehatmap on tableau.
*
*/
class ZoneAnalyzer(val sc:SparkContext,val sqlContext:SQLContext){
//This funciton finds out the percentage of rides which happen between 10 pm - 5 am throught the day for all the boroughs in NYC
	def findZonePercent(taxiRDD:org.apache.spark.rdd.RDD[Seq[String]],baseRDD:org.apache.spark.rdd.RDD[(String,(String,String))])={
		import sqlContext._
		import sqlContext.implicits._
		val set = Set("2017","2018","2019")
		val set2 = Set("22-23","23-0","0-1","1-2","2-3","3-4","4-5")
		val countRDD = taxiRDD.filter(line=>set.contains(line(8).substring(0,4)))
									 .map(line=>(line(3),(line(8),line(9))))
		val taxiModifiedRDD = taxiRDD.filter(line=>set.contains(line(8).substring(0,4)))
									 .filter(line=>set2.contains(line(9)))
									 .map(line=>(line(3),(line(8),line(9))))
		val tempRDD = countRDD.join(baseRDD)
		val zoneRDD = taxiModifiedRDD.join(baseRDD)
		val boroughCount = tempRDD.map(line=>(line._2._2._2,1)).reduceByKey((v1,v2)=>v1+v2)
		var count= scala.collection.mutable.Map[String, Int]()
		var countArr = boroughCount.collect()
		countArr.foreach(elem=>count(elem._1)=elem._2)
		val percentRDD = zoneRDD.map(line=>(line._2._2._2,(line._2._2._1,1)))
								.reduceByKey((v1,v2)=>(v1._1,v1._2+v2._2))
								.map(line=>(line._1,(100.00*line._2._2/count(line._1))))
								.map(line=>Row(line._1,line._2))
		val schema = StructType(
				List(
					StructField("Zone",StringType,true),
					StructField("LateNightRides",FloatType,true)
					)
				)
		val zoneDF=sqlContext.createDataFrame(percentRDD,schema)		
		zoneDF.write.saveAsTable("sg6148.zoneAnalysis_Final")
	}		
}