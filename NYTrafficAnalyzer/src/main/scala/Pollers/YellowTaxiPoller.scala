package Pollers
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
class YellowTaxiPoller(val sc:SparkContext) {
  def poll0914: org.apache.spark.rdd.RDD[String]={
  	val file1 = "project/yellowTaxi/2009*"
  	val file2 = "project/yellowTaxi/2010*"
  	val file3 = "project/yellowTaxi/2011*"
  	val file4 = "project/yellowTaxi/2012*"
  	val file5 = "project/yellowTaxi/2013*"
  	val file6 = "project/yellowTaxi/2014*"
  	val yellowRDD1 = sc.textFile(file1)
  	val yellowRDD2 = sc.textFile(file2)
  	val yellowRDD3 = sc.textFile(file3)
  	val yellowRDD4 = sc.textFile(file4)
  	val yellowRDD5 = sc.textFile(file5)
  	val yellowRDD6 = sc.textFile(file6)
  	var yellowRDD = yellowRDD1.union(yellowRDD2)
  	yellowRDD = yellowRDD.union(yellowRDD3)
  	yellowRDD = yellowRDD.union(yellowRDD4)
  	yellowRDD = yellowRDD.union(yellowRDD5)
  	yellowRDD = yellowRDD.union(yellowRDD6)
  	yellowRDD
  }
  def poll1516: org.apache.spark.rdd.RDD[String]={
  	val file1 = "project/yellowTaxi/2015*"
  	val file2 = "project/yellowTaxi/2016*"
  	val yellowRDD1 = sc.textFile(file1)
  	val yellowRDD2 = sc.textFile(file2)
  	var yellowRDD = yellowRDD1.union(yellowRDD2)
  	yellowRDD
  }
  def poll1718: org.apache.spark.rdd.RDD[String]={
  	val file1 = "project/yellowTaxi/2017*"
  	val file2 = "project/yellowTaxi/2018*"
  	val yellowRDD1 = sc.textFile(file1)
  	val yellowRDD2 = sc.textFile(file2)
  	var yellowRDD = yellowRDD1.union(yellowRDD2)
  	yellowRDD
  }
  def poll19: org.apache.spark.rdd.RDD[String]={
  	val file1 = "project/yellowTaxi/2019*"
  	val yellowRDD = sc.textFile(file1)
  	yellowRDD
  }    

}