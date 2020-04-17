package Analyzer
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
class CountAnalyzer(val sqlContext : SQLContext,
					val fhvRDD: org.apache.spark.rdd.RDD[Seq[String]],
					val licenseNumRDD: org.apache.spark.rdd.RDD[String]){

	def createTotalYearCount(taxiRDD:org.apache.spark.rdd.RDD[Seq[String]])={
		val tempRDD = taxiRDD.map(line => (line(8).substring(0,7),1))
		val countRDD = tempRDD.reduceByKey((v1,v2)=>v1+v2)
		countRDD
	}

	var fn2label= scala.collection.mutable.Map[String, String]()

	def checkIfKeyInMap(arr: Seq[String])={
			if(arr(0).equals("-1") && fn2label.contains(arr(3))==false) false
			true
	}

	def getYearMon(str: String)={
			var index: Int = -1
			var trimmedDate: String = str
			index=str.lastIndexOf("-")
			if(index>0){
				trimmedDate=str.substring(0,index)
			} 
			trimmedDate
	}

	def getNewScehma(arr : Seq[String])={
		if(arr(0).equals("-1")){
			
			if(fn2label.contains(arr(3))==false)
				arr.updated(0,"other")
			else{
				if(fn2label(arr(3)).contains("uber"))
					arr.updated(0,"uber")
				else if(fn2label(arr(3)).contains("via"))
					arr.updated(0,"via")
				else if(fn2label(arr(3)).contains("lyft"))
					arr.updated(0,"lyft")
				else if(fn2label(arr(3)).contains("juno"))
					arr.updated(0,"juno")
				else
					arr.updated(0,"other")
			}
		}
		else{
			if(fn2label.contains(arr(0)))
				arr.updated(0,fn2label(arr(0)))
			else
				arr.updated(0,"other")
			}
	}

	def consolidateFHVdataWithMappings={

		val labels: Array[Array[String]] = licenseNumRDD.map(line => line.split(","))
											.filter(line=> line(0).length>0 && line(3).length>0)
											.collect()

		labels.foreach{x => fn2label += (x(0) -> x(3).toLowerCase())}
		fn2label+=("HV0002"->"juno")
		fn2label+=("HV0003" -> "uber")
		fn2label+=("HV0004" -> "via")
		fn2label+=("HV0005" -> "lyft")
 	 	
 	 	fhvRDD.filter(line=>checkIfKeyInMap(line)).map(line=>getNewScehma(line)).take(10)
		fhvRDD
	}
	
	def getTaxiRDDbyName(finalRDD: org.apache.spark.rdd.RDD[Seq[String]], nameOfTaxi: String )={

		val taxiByNameRDD = finalRDD.map(line=>getNewScehma(line))
					   		  		.filter(line=>line(0).equals(nameOfTaxi))
					   		 		.map(line=>(getYearMon(line(4)),1))
					   		 		.reduceByKey(_+_)
					   		  		.sortByKey()

		taxiByNameRDD		   		  		
	}

	def createSQLTable(yellowtaxi:org.apache.spark.rdd.RDD[Seq[String]],
					   greentaxi:org.apache.spark.rdd.RDD[Seq[String]]
					   )={
		import sqlContext._
		import sqlContext.implicits._
		val yellowtaxiRDD = createTotalYearCount(yellowtaxi)
		val greentaxiRDD = createTotalYearCount(greentaxi)
		val fhvRDD = consolidateFHVdataWithMappings
		val uberRdd = getTaxiRDDbyName(fhvRDD,"uber")
		val lyftRdd = getTaxiRDDbyName(fhvRDD,"lyft")
		val junoRdd = getTaxiRDDbyName(fhvRDD,"juno")
		val viaRdd = getTaxiRDDbyName(fhvRDD,"via")
		val finalRDD = yellowtaxiRDD.leftOuterJoin(greentaxiRDD)
					    .map(line=>(line._1,(line._2._1,getVal(line._2._2))))
		val finalRDD1 = finalRDD.leftOuterJoin(uberRdd)
					    .map(line=>(line._1,(line._2._1._1,line._2._1._2,getVal(line._2._2))))
		val finalRDD2 = finalRDD1.leftOuterJoin(lyftRdd)
						.map(line=>(line._1,(line._2._1._1,line._2._1._2,line._2._1._3,getVal(line._2._2))))
		val finalRDD3 = finalRDD2.leftOuterJoin(junoRdd)
						.map(line=>(line._1,(line._2._1._1,line._2._1._2,line._2._1._3,line._2._1._4,getVal(line._2._2))))
		val finalRDD4 = finalRDD3.leftOuterJoin(viaRdd)
						.map(line=>(line._1,(line._2._1._1,line._2._1._2,line._2._1._3,line._2._1._4,line._2._1._5,getVal(line._2._2))))
						.sortByKey()
		val transitRDD = finalRDD4.map(line => Row(line._1,line._2._1,line._2._2,line._2._3,line._2._4,line._2._5,line._2._6))
		val schema = StructType(
						List(
							StructField("Year",StringType,true),
							StructField("YellowTaxiCount",IntegerType,true),
							StructField("GreenTaxiCount",IntegerType,true),
							StructField("UberCount",IntegerType,true),
							StructField("LyftCount",IntegerType,true),
							StructField("JunoTaxiCount",IntegerType,true),
							StructField("ViaTaxiCount",IntegerType,true)
						)
					)
		val countDF = sqlContext.createDataFrame(transitRDD,schema)
		countDF.write.saveAsTable("sg6148.Analysis1_1");
	}

	def getVal(value:Option[Int])={
		if(value!=None)value.get
		else null
	} 

}