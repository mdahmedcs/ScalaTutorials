package com.code.scala
import scala.collection.immutable.HashSet 
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object First{  
    def main(args:Array[String]): Unit={  

   val spark:SparkSession = SparkSession.builder().master("local[1]")
          .appName("SparkByExamples.com")
          .getOrCreate()
      val rdd:RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5))
      rdd.collect().foreach(println)

    }
} 