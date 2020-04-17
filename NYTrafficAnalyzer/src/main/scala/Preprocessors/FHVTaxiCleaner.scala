package Preprocessors
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv
import org.apache.spark.sql.functions._
import java.io.Serializable
class FHVTaxiCleaner extends Serializable {    
    
    def clean1516(data1 : org.apache.spark.rdd.RDD[String]) ={
        val postFilteredrddData=data1.map(_.split(","))
                                .filter(line=>line(0)!="Dispatching_base_num")
                                .map(line=>getNewScehma1516(line))
        postFilteredrddData
    }
    def clean17(data1 : org.apache.spark.rdd.RDD[String]) ={
        val postFilteredrddData= data1.map(_.replace("\"",""))
                                .map(_.split(","))
                                .filter(line=>line(0)!="Dispatching_base_num")
                                .map(line=>getNewSchema17(line))
        postFilteredrddData
    }
    def clean18(data1 : org.apache.spark.rdd.RDD[String]) ={
        val postFilteredrddData=data1.map(_.replace("\"",""))
                                .map(_.split(","))
                                .filter(_.length==6)
                                .map(line=>getNewSchema18(line))
        postFilteredrddData
    }
    def clean19(data1 : org.apache.spark.rdd.RDD[String]) ={
        val postFilteredrddData=data1.map(_.split(","))
                                .filter(line=>line(0)!="hvfhs_license_num")
                                .map(line=>getNewSchema19(line))
        postFilteredrddData
    }
    def clean(data0 : org.apache.spark.rdd.RDD[String], data1 : org.apache.spark.rdd.RDD[String],data2 : org.apache.spark.rdd.RDD[String],data3 : org.apache.spark.rdd.RDD[String],data4 : org.apache.spark.rdd.RDD[String])={
    
        var rdd15 = clean1516(data0)
        var rdd16 = clean1516(data1)
        val rdd17 = clean17(data2)
        val rdd18 = clean18(data3)
        val rdd19 = clean19(data4)
        var rdda = rdd15.union(rdd16)
        rdda = rdda.union(rdd17)
        rdda = rdda.union(rdd18)
        rdda = rdda.union(rdd19)
        rdda
    }


    def getNewSchema19(arr:Array[String])={
    var newSchema = Seq.empty[String]
    var taxiMaaping="-1"
    var tripStartDate=""
    var tripStartTime=""
    var tripEndDate=""
    var tripEndTime=""
    var license="-1"
    var pickUpId="-1"
    var dropOffId="-1"
    for(i<- 0 to arr.length-1){
        if(i==0){
            if(arr(i).length>0)
                taxiMaaping=arr(i)
            newSchema=newSchema:+taxiMaaping
        }
        else if(i==1){
            if(arr(i).length>0)
                license=arr(i)
        }
        else if(i==2){
            if(arr(i).split(" ").length==2){
                        tripStartDate = arr(i).split(" ")(0)
                        tripStartTime = arr(i).split(" ")(1)
                    }
                    else if(arr(i).split(" ").length==1 && arr(i).length>=4){
                        if(arr(i).substring(0,5).contains(":"))
                            tripStartTime=arr(i)
                        else
                            tripStartDate=arr(i)
                    }
        }   
        else if(i==3){
            if(arr(i).split(" ").length==2){
                        tripEndDate = arr(i).split(" ")(0)
                        tripEndTime = arr(i).split(" ")(1)
                    }
                 else if(arr(i).split(" ").length==1 && arr(i).length>=4){
                        if(arr(i).substring(0,5).contains(":"))
                            tripEndTime = arr(i)
                        else
                            tripEndDate = arr(i)
                    }    
        }
        else if(i==4){  
            if(arr(i).length>0)
                pickUpId=arr(i)
        }
        else if(i==5){
            if(arr(i).length>0)
               dropOffId=arr(i)
        }
    }
        newSchema=newSchema:+pickUpId
        newSchema=newSchema:+dropOffId
        newSchema=newSchema:+license
        newSchema=newSchema:+tripStartDate
        newSchema=newSchema:+tripStartTime
        newSchema=newSchema:+tripEndDate
        newSchema=newSchema:+tripEndTime
    newSchema
}
def getNewSchema18(arr:Array[String])={
    var newSchema = Seq.empty[String]
    var taxiMaaping="-1"
    var tripStartDate=""
    var tripStartTime=""
    var tripEndDate=""
    var tripEndTime=""
    var pickUpId="-1"
    var dropOffId="-1"
    var license="-1"
    newSchema=newSchema:+taxiMaaping

    for(i<- 0 to arr.length-1){
        if(i==0){
            tripStartDate = arr(i).split(" ")(0)
            tripStartTime = arr(i).split(" ")(1)
        }   
        else if(i==1){
            tripEndDate = arr(i).split(" ")(0)
            tripEndTime = arr(i).split(" ")(1)    
        }
        else if(i==2){
            if(arr(i).length>0)
                pickUpId=arr(i)
        }
        else if(i==3){
            if(arr(i).length>0)
                 dropOffId=arr(i)
        }
        else if(i==5){   
         if(arr(i).length>0)
            license=arr(i)
        }
    }
    
        newSchema=newSchema:+pickUpId
        newSchema=newSchema:+dropOffId
        newSchema=newSchema:+license
        newSchema=newSchema:+tripStartDate
        newSchema=newSchema:+tripStartTime
        newSchema=newSchema:+tripEndDate
        newSchema=newSchema:+tripEndTime
    newSchema
}

def getNewSchema17(arr:Array[String])={
        var newSchema = Seq.empty[String]
        var taxiMaaping="-1"
        var tripStartDate="-1"
        var tripStartTime="-1"
        var tripEndDate="-1"
        var tripEndTime="-1"
        var pickUpId="-1"
        var dropOffId="-1"
        var license="-1"

        if(arr.length>0){
        for(i<- 0 to arr.length-1){
            if(i==1){
                if(arr(i).length==0){
                    tripStartDate = "-1"
                    tripStartTime = "-1"
                }
            else{
                    if(arr(i).split(" ").length==2){
                        tripStartDate = arr(i).split(" ")(0)
                        tripStartTime = arr(i).split(" ")(1)
                    }
                    else if(arr(i).split(" ").length==1 && arr(i).length>=4){
                        if(arr(i).substring(0,5).contains(":"))
                            tripStartTime=arr(i)
                        else
                            tripStartDate=arr(i)
                    }
                }
            }
            if(i==2){
                if(arr(i).length==0){
                    tripEndDate = "-1"  
                    tripEndTime = "-1" 
                }
            else{
                 if(arr(i).split(" ").length==2){
                        tripEndDate = arr(i).split(" ")(0)
                        tripEndTime = arr(i).split(" ")(1)
                    }
                 else if(arr(i).split(" ").length==1 && arr(i).length>=4){
                        if(arr(i).substring(0,5).contains(":"))
                            tripEndTime = arr(i)
                        else
                            tripEndDate = arr(i)
                    }
            }   
        }
        if(i==0 || i==3 || i==4){
            if(arr(i).length>0){
                if(i==0)
                    license=arr(i)
                else if(i==3)
                    pickUpId=arr(i)
                else if(i==4)
                    dropOffId=arr(i)
            }
            
        }
    }
}
        newSchema=newSchema:+taxiMaaping
        newSchema=newSchema:+pickUpId
        newSchema=newSchema:+dropOffId
        newSchema=newSchema:+license
        newSchema=newSchema:+tripStartDate
        newSchema=newSchema:+tripStartTime
        newSchema=newSchema:+tripEndDate
        newSchema=newSchema:+tripEndTime
        newSchema
}

def getNewScehma1516(arr:Array[String])={
        var newSchema = Seq.empty[String]
        var taxiMaaping="-1"
        var tripStartDate="-1"
        var tripStartTime="-1"
        var tripEndDate="-1"
        var tripEndTime="-1"
        var pickUpId="-1"
        var dropOffId="-1"
        var license="-1"
        for(i<- 0 to arr.length-1){
            if(i==0){
                license=arr(i)
            }
             else if(i==1){
                if(arr(i).split(" ").length==2){
                    tripStartDate = arr(i).split(" ")(0)
                    tripStartTime = arr(i).split(" ")(1)
                }
                else if(arr(i).split(" ").length==1 && arr(i).length>=4){
                    if(arr(i).substring(0,5).contains(":"))
                        tripStartTime=arr(i)
                    else
                        tripStartDate=arr(i)
                }
            }
            else{
                if(arr(i).length>0)
                    pickUpId=arr(i)
            }
        }

        newSchema=newSchema:+taxiMaaping
        newSchema=newSchema:+pickUpId
        newSchema=newSchema:+dropOffId
        newSchema=newSchema:+license
        newSchema=newSchema:+tripStartDate
        newSchema=newSchema:+tripStartTime
        newSchema=newSchema:+tripEndDate
        newSchema=newSchema:+tripEndTime
        newSchema
}

}
