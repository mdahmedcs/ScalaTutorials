package com.code.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDDEMO{  
    def main(args:Array[String]): Unit={  

  val spark=SparkSession.builder().master("local[4]").appName("rdd demo").getOrCreate()
  
  val sc= spark.sparkContext
  
  //Method1: Parallelizing a collection
  val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15))
      
      //  rdd.repartition(2)
      rdd.cache()
      
      
      println("print elements of rdd")
      rdd.collect().foreach(println)
      
      println("map function")
      rdd.map(a=>a*a).collect().foreach(println)
      
      println("filter function")
      rdd.filter(a=>a!=11).collect().foreach(println)
      
      rdd.union(rdd).collect().foreach(println)
      
      println("number of partitions "+rdd.getNumPartitions)
      
      println("First " +rdd.first())
      
      // Method2: Reading from files: Text, CSV, 
      
     val rdd2=sc.textFile("C:\\Users\\ahmed\\Downloads\\subscribers.csv,C:\\Users\\ahmed\\Downloads\\subscribers.csv") //reading from multiple files
     
     rdd2.collect().take(10).foreach(println)
     println("count: "+rdd2.count())
      
      
      

    }
} 