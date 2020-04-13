package Pollers
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
class GreenTaxiPoller(val sc:SparkContext) {
  def poll1314: org.apache.spark.rdd.RDD[String]={
  	val file1 = "project/greenTaxi/2013*"
  	val file2 = "project/greenTaxi/2014*"
  	val greenRDD1 = sc.textFile(file1)
  	val greenRDD2 = sc.textFile(file2)
  	var greenRDD = greenRDD1.union(greenRDD2)
  	greenRDD
  }
  def poll1516: org.apache.spark.rdd.RDD[String]={
  	val file1 = "project/greenTaxi/2015*"
  	val file2 = "project/greenTaxi/2016*"
  	val greenRDD1 = sc.textFile(file1)
  	val greenRDD2 = sc.textFile(file2)
  	var greenRDD = greenRDD1.union(greenRDD2)
  	greenRDD
  }
  def poll1718: org.apache.spark.rdd.RDD[String]={
  	val file1 = "project/greenTaxi/2017*"
  	val file2 = "project/greenTaxi/2018*"
  	val greenRDD1 = sc.textFile(file1)
  	val greenRDD2 = sc.textFile(file2)
  	var greenRDD = greenRDD1.union(greenRDD2)
  	greenRDD
  }
  def poll19: org.apache.spark.rdd.RDD[String]={
  	val file1 = "project/greenTaxi/2019*"
  	val greenRDD = sc.textFile(file1)
  	greenRDD
  }    

}