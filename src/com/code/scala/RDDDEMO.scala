package com.code.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDDEMO{  
    def main(args:Array[String]): Unit={  

   val spark = SparkSession.builder().master("local[1]")
          .appName("SparkByExamples.com")
          .getOrCreate()
      val rdd:RDD[Int] = spark.sparkContext.parallelize(List(11,12,1,2,3,4,5))
      
      println("print elements of rdd")
      rdd.collect().foreach(println)
      
      println("map function")
      rdd.map(a=>a*a).collect().foreach(println)
      
      println("filter function")
      rdd.filter(a=>a!=11).collect().foreach(println)
      
      
      
      
      

    }
} 