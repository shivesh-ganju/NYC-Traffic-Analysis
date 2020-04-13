package Pollers
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
class FHVPoller(val sqlContext:SQLContext) {

def poll= {
	import sqlContext._
	import sqlContext.implicits._
	val file = "project/fhvTaxi/*.csv"
	val fhvDF = sqlContext.read.format("com.databricks.spark.csv").option("header","true").load(file)
	fhvDF
	}
}