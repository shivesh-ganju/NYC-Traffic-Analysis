package Analyzer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.io.Serializable
// This class "WeatherAnalyzer" generates trends in conjunction with weather and it's effects 
// on taxi services, pickup Trends, fare trends analysis.
class WeatherAnalyzer(val sqlContext:SQLContext) extends Serializable{

// This method takes in cleanYellowRDD and cleanedWeatherRdd 
// and generates average pick ups with Snow and Rain Intensities across the 
// years 2010 to 2016. Firstly we use reduceByKey to get total pick ups for each date
// and join that with weather data to get daily basis weather conditions and fare amount.
// Post that average picksups generated for all days corresponding to a snow and rain Intensities
// have been calculated. Finally a dataframe is created from the RDD
// and then written on the HDFS. The final result is saved as a HIVE table in HDFS. 
  def generateAvgPickUpsYellowTaxiWeatherSnowRain(yellowRDD:org.apache.spark.rdd.RDD[Seq[String]],weatherRDD:org.apache.spark.rdd.RDD[(String, (String, String))])={
    import sqlContext._
    import sqlContext.implicits._

    val years = Set("2010","2011","2012","2013","2014","2015","2016")
    val yellowTaxiPickUpsRdd = yellowRDD.filter(line=>years.contains(line(8).substring(0,4)))
                                        .map(line=>(line(8),1))
                                        .reduceByKey((v1,v2)=>v1+v2)

    val snowDateRdd = weatherRDD.map(line=>(line._1,line._2._2)) 
    val rainDateRdd = weatherRDD.map(line=>(line._1,line._2._1))                               
    val combinedSnowPickUpsRDD = yellowTaxiPickUpsRdd.join(snowDateRdd)
    val combinedRainPickUpsRDD = yellowTaxiPickUpsRdd.join(rainDateRdd)

    val snowYTaxiPickUpRdd = combinedSnowPickUpsRDD.map(line=>(line._2._2,(line._2._1,1)))
                                        .reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2))
                                        .map(line=>(line._1,line._2._1/line._2._2))
                                        

    val rainYTaxiPickUpRdd = combinedRainPickUpsRDD.map(line=>(line._2._2,(line._2._1,1)))
                                        .reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2))
                                        .map(line=>(line._1,line._2._1/line._2._2))
                                        

    println("Inside PickUpsWeather")
    val snowPickUpRDD = snowYTaxiPickUpRdd.map(line => Row(line._1,line._2))
    val schemaSnow = StructType(
            List(
              StructField("SnowFall_Intensity",StringType,true),
              StructField("Average_Pickups",IntegerType,true)
            )
          )
    val countDFSnow = sqlContext.createDataFrame(snowPickUpRDD,schemaSnow)
    val finalDFSnow = countDFSnow.sort($"Average_Pickups".desc)

    val rainPickUpRDD = rainYTaxiPickUpRdd.map(line => Row(line._1,line._2))
    val schemaRain = StructType(
            List(
              StructField("Rain_Intensity",StringType,true),
              StructField("Average_Pickups",IntegerType,true)
            )
          )
    val countDFRain = sqlContext.createDataFrame(rainPickUpRDD,schemaRain)
    val finalDFRain = countDFRain.sort($"Average_Pickups".desc)

    finalDFSnow.write.saveAsTable("rs6980.Analysis1_1_Final")
    finalDFRain.write.saveAsTable("rs6980.Analysis1_2_Final")
  }

// This method takes in cleanYellowRDD and cleanedWeatherRdd 
// and generates average fare collected with respect to Snow and Rain Intensities across the 
// years 2010 to 2016. Firstly we use reduceByKey to get total fare amount for each date
// and join that with weather data to get daily basis weather conditions and fare amount.
// Post that average picksups generated for all days corresponding to a snow and rain Intensities
// have been calculated. Finally a dataframe is created from the RDD
// and then written on the HDFS. The final result is saved as a HIVE table in HDFS. 
  def generateAvgDailyFareAmount(yellowRDD:org.apache.spark.rdd.RDD[Seq[String]],weatherRDD:org.apache.spark.rdd.RDD[(String, (String, String))]){

    import sqlContext._
    import sqlContext.implicits._

    val years = Set("2010","2011","2012","2013","2014","2015","2016")
    val yellowTaxiPriceDateRdd = yellowRDD.filter(line=>years.contains(line(8).substring(0,4)))
                                          .map(line=>(line(8),line(7).toFloat))
                                          .reduceByKey((v1,v2)=> v1+v2)

    val snowDateRdd = weatherRDD.map(line=>(line._1,line._2._2)) 
    val combinedSnowPickUpsRDD = yellowTaxiPriceDateRdd.join(snowDateRdd)

    val avgDailyFareAmountSnow = combinedSnowPickUpsRDD.map(line=>(line._2._2,(line._2._1,1)))
                                                       .reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2))
                                                       .map(line=>(line._1,line._2._1/line._2._2))
                                                       
   
    val avgFareSnowRDD = avgDailyFareAmountSnow.map(line => Row(line._1,line._2))
    val schemaAvgSnowFare = StructType(
            List(
              StructField("SnowFall_Intensity",StringType,true),
              StructField("Average_FareAmount",FloatType,true)
            )
          )
    val countDFfare = sqlContext.createDataFrame(avgFareSnowRDD,schemaAvgSnowFare)
    val finalDF = countDFfare.sort($"Average_FareAmount".desc)

    finalDF.write.saveAsTable("rs6980.Analysis2_1_Final")

  }

}