package Preprocessors
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv
import org.apache.spark.sql.functions._
import java.io.Serializable
class YellowTaxiCleaner extends Serializable {    
    def clean0914(data1 : org.apache.spark.rdd.RDD[String]) ={
        val a = Set("2009","2010","2011","2012","2013","2014")
        val rdd11=data1.map(_.split(","))
            .filter(_.length==18)
            .filter(line=>line(0)!="vendor_id" && line(0)!="vendor_name")
            .map(modify0914(_))
            .filter(checkNA(_))
            .filter(line=>a.contains(line(8).substring(0,4)))
        rdd11
    }
    def clean1516(data1 : org.apache.spark.rdd.RDD[String]) ={
        val a = Set("2015","2016")
        val rdd11=data1.map(_.split(","))
            .filter(_.length==19)
            .filter(_(0)!="VendorID")
            .map(modify1516(_))
            .filter(checkNA(_))
            .filter(line=>a.contains(line(8).substring(0,4)))
        rdd11
    }
    def clean1718(data1 : org.apache.spark.rdd.RDD[String]) ={
        val a = Set("2017","2018")
        val rdd11=data1.map(_.split(","))
            .filter(_.length==17)
            .filter(_(0)!="VendorID")
            .map(modify1718(_))
            .filter(checkNA(_))
            .filter(line=>a.contains(line(8).substring(0,4)))
        rdd11
    }
    def clean19(data1 : org.apache.spark.rdd.RDD[String]) ={
        val a = Set("2019")
        val rdd11=data1.map(_.split(","))
            .filter(_.length==18)
            .filter(_(0)!="VendorID")
            .map(modify19(_))
            .filter(checkNA(_))
            .filter(line=>a.contains(line(8).substring(0,4)))
        rdd11
    }
    def clean(data1 : org.apache.spark.rdd.RDD[String],data2 : org.apache.spark.rdd.RDD[String],data3 : org.apache.spark.rdd.RDD[String],data4 : org.apache.spark.rdd.RDD[String])={
        val rdd0914 = clean0914(data1)
        val rdd1516 = clean1516(data2)
        val rdd1718 = clean1718(data3)
        val rdd19 = clean19(data4)
        var rdda = rdd0914.union(rdd1516)
        rdda = rdda.union(rdd1718)
        rdda= rdda.union(rdd19)
        //rdda.saveAsTextFile("project/clean_yellow_data2/")
        rdda
    }
    def modify0914(arr:Array[String])={
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
            else if(i!=7&&i!=8&&i!=11&&i!=12&&i!=13&&i!=14&&i!=15&&i!=16){
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
            else if(i!=7&&i!=8&&i!=11&&i!=12&&i!=13&&i!=14&&i!=15&&i!=16&&i!=17){
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
            else if(i!=5&&i!=6&&i!=9&&i!=10&&i!=11&&i!=12&&i!=13&&i!=14&&i!=15){
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
            else if(i!=5&&i!=6&&i!=9&&i!=10&&i!=11&&i!=12&&i!=13&&i!=14&&i!=15&&i!=17){
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


