package Pollers
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext

class FHVTaxiPoller(val sc:SparkContext) {
 
  def poll15: org.apache.spark.rdd.RDD[String]={
  	val file1 = "/user/rs6980/project/folder_fhv/2015*.csv"
  	val fhvRDD = sc.textFile(file1)
  	fhvRDD
  }
  def poll16: org.apache.spark.rdd.RDD[String]={
    val file1 = "/user/rs6980/project/folder_fhv/2016*.csv"
    val fhvRDD = sc.textFile(file1)
    fhvRDD
  }
  def poll17: org.apache.spark.rdd.RDD[String]={
  	val file1 = "/user/rs6980/project/folder_fhv/2017*.csv"
  	val fhvRDD = sc.textFile(file1)
  	fhvRDD
  }
  def poll18: org.apache.spark.rdd.RDD[String]={
  	val file1 = "/user/rs6980/project/folder_fhv/2018*.csv"
  	val fhvRDD = sc.textFile(file1)
  	fhvRDD
  }    
  def poll19: org.apache.spark.rdd.RDD[String]={
    val fhvRDD = sc.textFile("/user/rs6980/project/folder_fhvhv19/2019*.csv")
    fhvRDD
  }  

  def pollTaxiMapping: org.apache.spark.rdd.RDD[String]={
    val labeldataRDD = sc.textFile("/user/rs6980/project/fhvBases")
    labeldataRDD
  }
}