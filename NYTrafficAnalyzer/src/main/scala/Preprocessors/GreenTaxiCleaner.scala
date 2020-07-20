package Preprocessors
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv
import org.apache.spark.sql.functions._
import java.io.Serializable
/*
* This class cleans the Green taxi dataset and combines the different schemas into one uniform schema
*/
class GreenTaxiCleaner extends Serializable { 
//Cleans and filters the 2013-14 dataset
    def clean1314(data1 : org.apache.spark.rdd.RDD[String]) ={
        val a = Set("2013","2014")
        val rdd11=data1.map(_.split(","))
            .filter(_.length==20)
            .filter(line=>line(0)!="VendorID")
            .map(modify1314(_))
            .filter(checkNA(_))
            .filter(line=>a.contains(line(8).substring(0,4)))
        rdd11
    }
//Cleans and filters the 2015-16 dataset
    def clean1516(data1 : org.apache.spark.rdd.RDD[String]) ={
        val a = Set("2015","2016")
        val rdd11=data1.map(_.split(","))
            .filter(_.length==21)
            .filter(_(0)!="VendorID")
            .map(modify1516(_))
            .filter(checkNA(_))
            .filter(line=>a.contains(line(8).substring(0,4)))
        rdd11
    }
//Cleans and filters the 2017-14 dataset
    def clean1718(data1 : org.apache.spark.rdd.RDD[String]) ={
        val a = Set("2017","2018")
        val rdd11=data1.map(_.split(","))
            .filter(_.length==19)
            .filter(_(0)!="VendorID")
            .map(modify1718(_))
            .filter(checkNA(_))
            .filter(line=>a.contains(line(8).substring(0,4)))
        rdd11
    }
//Cleans and filters the 2019 dataset
    def clean19(data1 : org.apache.spark.rdd.RDD[String]) ={
        val a = Set("2019")
        val rdd11=data1.map(_.split(","))
            .filter(_.length==20)
            .filter(_(0)!="VendorID")
            .map(modify19(_))
            .filter(checkNA(_))
            .filter(line=>a.contains(line(8).substring(0,4)))
        rdd11
    }
// Combines all the individually cleaned dataset and merges them into one rdd
    def clean(data1 : org.apache.spark.rdd.RDD[String],data2 : org.apache.spark.rdd.RDD[String],data3 : org.apache.spark.rdd.RDD[String],data4 : org.apache.spark.rdd.RDD[String])={
        val rdd1314 = clean1314(data1)
        val rdd1516 = clean1516(data2)
        val rdd1718 = clean1718(data3)
        val rdd19 = clean19(data4)
        var rdda = rdd1314.union(rdd1516)
        rdda = rdda.union(rdd1718)
        rdda= rdda.union(rdd19)
        //rdda.saveAsTextFile("project/clean_green_data/")
        rdda
    }
// Modifies the dataset for 2013-2014 into a uniform schema
    def modify1314(arr:Array[String])={
        var arr_new = Seq.empty[String]
        var date_start=""
        var time_start=""
        var date_end=""
        var time_end=""
        for(i<- 0 to arr.length-1){
            if(i==1){
            date_start = arr(i).split(" ")(0)
            time_start = arr(i).split(" ")(1)
            }
            else if(i==2){
            date_end = arr(i).split(" ")(0)
            time_end = arr(i).split(" ")(1)    
            }
            else if(i!=3&&i!=4&&i!=11&&i!=12&&i!=13&&i!=14&&i!=15&&i!=16&&i!=18&&i!=19){
            arr_new=arr_new:+arr(i)
            }
        }
        val time_start_bucket = ((time_start.substring(0,2).toInt)%24).toString +"-" + ((time_start.substring(0,2).toInt + 1)%24).toString
        val time_end_bucket = ((time_end.substring(0,2).toInt)%24).toString +"-" + ((time_end.substring(0,2).toInt + 1)%24).toString
        arr_new=arr_new:+date_start
        arr_new=arr_new:+time_start_bucket
        arr_new=arr_new:+date_end
        arr_new=arr_new:+time_end_bucket
        arr_new
    }
// Modifies the dataset for 2015-2016 into a uniform schema
    def modify1516(arr:Array[String])={
        var arr_new = Seq.empty[String]
        var date_start=""
        var time_start=""
        var date_end=""
        var time_end=""
        for(i<- 0 to arr.length-1){
            if(i==1){
            date_start = arr(i).split(" ")(0)
            time_start = arr(i).split(" ")(1)
            }
            else if(i==2){
            date_end = arr(i).split(" ")(0)
            time_end = arr(i).split(" ")(1)    
            }
            else if(i!=3&&i!=4&&i!=11&&i!=12&&i!=13&&i!=14&&i!=15&&i!=16&&i!=17&&i!=19&&i!=20){
            arr_new=arr_new:+arr(i)
            }
        }
        val time_start_bucket = ((time_start.substring(0,2).toInt)%24).toString +"-" + ((time_start.substring(0,2).toInt + 1)%24).toString
        val time_end_bucket = ((time_end.substring(0,2).toInt)%24).toString +"-" + ((time_end.substring(0,2).toInt + 1)%24).toString
        arr_new=arr_new:+date_start
        arr_new=arr_new:+time_start_bucket
        arr_new=arr_new:+date_end
        arr_new=arr_new:+time_end_bucket
        arr_new
    }
// Modifies the dataset for 2017-2018 into a uniform schema
    def modify1718(arr:Array[String])={
        var arr_new = Seq.empty[String]
        var date_start=""
        var time_start=""
        var date_end=""
        var time_end=""
        for(i<- 0 to arr.length-1){
            if(i==1){
            date_start = arr(i).split(" ")(0)
            time_start = arr(i).split(" ")(1)
            }
            else if(i==2){
            date_end = arr(i).split(" ")(0)
            time_end = arr(i).split(" ")(1)    
            }
            else if(i!=3&&i!=4&&i!=11&&i!=12&&i!=13&&i!=14&&i!=15&&i!=9&&i!=17&&i!=18&&i!=10){
                if(i==5||i==6){
                    arr_new=arr_new:+arr(i)
                }
                arr_new=arr_new:+arr(i)
            }
        }
        val time_start_bucket = ((time_start.substring(0,2).toInt)%24).toString +"-" + ((time_start.substring(0,2).toInt + 1)%24).toString
        val time_end_bucket = ((time_end.substring(0,2).toInt)%24).toString +"-" + ((time_end.substring(0,2).toInt + 1)%24).toString
        arr_new=arr_new:+date_start
        arr_new=arr_new:+time_start_bucket
        arr_new=arr_new:+date_end
        arr_new=arr_new:+time_end_bucket
        arr_new
    }
// Modifies the dataset for 2019 into a uniform schema
    def modify19(arr:Array[String])={
        var arr_new = Seq.empty[String]
        var date_start=""
        var time_start=""
        var date_end=""
        var time_end=""
        for(i<- 0 to arr.length-1){
            if(i==1){
            date_start = arr(i).split(" ")(0)
            time_start = arr(i).split(" ")(1)
            }
            else if(i==2){
            date_end = arr(i).split(" ")(0)
            time_end = arr(i).split(" ")(1)    
            }
            else if(i!=3&&i!=4&&i!=11&&i!=12&&i!=13&&i!=14&&i!=15&&i!=9&&i!=17&&i!=18&&i!=10&&i!=19){
                if(i==7||i==8){
                    arr_new=arr_new:+arr(i)
                }
                arr_new=arr_new:+arr(i)
            }
        }
        val time_start_bucket = ((time_start.substring(0,2).toInt)%24).toString +"-" + ((time_start.substring(0,2).toInt + 1)%24).toString
        val time_end_bucket = ((time_end.substring(0,2).toInt)%24).toString +"-" + ((time_end.substring(0,2).toInt + 1)%24).toString
        arr_new=arr_new:+date_start
        arr_new=arr_new:+time_start_bucket
        arr_new=arr_new:+date_end
        arr_new=arr_new:+time_end_bucket
        arr_new
    }
    def checkNA(arr : Seq[String])={
        for(i<-arr){
            if(i.length==0)false
        }
        true
    }

}