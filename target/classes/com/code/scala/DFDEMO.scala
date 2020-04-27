package com.code.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg

object DFDEMO {
  
  def main(args:Array[String]): Unit={
  
 
  
  val spark:SparkSession = SparkSession.builder().master("local[4]").appName("demo").getOrCreate()
  val sc = spark.sparkContext
  
  import spark.sqlContext.implicits._
  
  sc.setLogLevel("ERROR")
  
 val sample=Seq(Row("ahmed", 100,5000), Row("mike", 200,5000), Row("john", 300,15000), Row("nick", 400,10000), Row("michael", 500,15000))
 val samplerdd=sc.parallelize(sample)
 
 //creating schema
 val fields = List(StructField("name", StringType, nullable=true),StructField("id", IntegerType, nullable=true),StructField("salary", IntegerType, nullable=true))  //Notice Struct Field has three parameters: name, type and nullable
 val schema = StructType(fields)
 
 //creating df by passing rdd and schema arguments in createDataFrame method
 var DF=spark.createDataFrame(samplerdd,schema)  //here StrucType takes array of StructFields 
 
 //withColumn 
 val DF2=DF.withColumn("multiplication", col("salary")*100)
 DF2.show()
 
 //withColumn using when otherwise
  val DFWhen = DF.withColumn("dept", when(col("id")<=200, "IT").when(col("id")>200 and col("id")<500, "Network").otherwise("other"))
  DFWhen.show();
  
  
  //filter/where
  val DFFilter = DF.filter(col("id")<300)
  DFFilter.show()
 
  //min, max
 println("min salary "+DF.select(min(col("salary"))))
 println("max salary "+DF.select(max(col("salary"))))
 println("sum salary "+DF.select(sum(col("salary"))))
 println("avg salary "+DF.select(avg(col("salary"))))
  
  
  
  
}

}