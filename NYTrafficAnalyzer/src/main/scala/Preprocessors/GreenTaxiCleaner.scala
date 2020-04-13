package Preprocessors
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv
import org.apache.spark.sql.functions._
import java.io.Serializable
class GreenTaxiCleaner extends Serializable {    
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
        arr_new=arr_new:+date_start
        arr_new=arr_new:+time_start
        arr_new=arr_new:+date_end
        arr_new=arr_new:+time_end
        arr_new
    }
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
        arr_new=arr_new:+date_start
        arr_new=arr_new:+time_start
        arr_new=arr_new:+date_end
        arr_new=arr_new:+time_end
        arr_new
    }
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
        arr_new=arr_new:+date_start
        arr_new=arr_new:+time_start
        arr_new=arr_new:+date_end
        arr_new=arr_new:+time_end
        arr_new
    }
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
        arr_new=arr_new:+date_start
        arr_new=arr_new:+time_start
        arr_new=arr_new:+date_end
        arr_new=arr_new:+time_end
        arr_new
    }
    def checkNA(arr : Seq[String])={
        for(i<-arr){
            if(i.length==0)false
        }
        true
    }

}