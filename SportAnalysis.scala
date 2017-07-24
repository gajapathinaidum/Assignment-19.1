package com.spark.sql.prctice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum

object SportAnalysis extends App {
 var conf=new SparkConf().setAppName("firs ex").setMaster("local");
 var sc=new SparkContext(conf)
 val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 val rdd = sc.textFile("C:\\SparkInputs\\Sports_Data.txt")
 val headerColumns = rdd.first().split(",").to[List]  
 
 def dfSchema(columnNames: List[String]): StructType = {
  StructType(
    Seq(
      StructField(name = "firstname", dataType = StringType, nullable = false),
      StructField(name = "lastname", dataType = StringType, nullable = false),
      StructField(name = "sports", dataType = StringType, nullable = false),
      StructField(name = "medal_type", dataType = StringType, nullable = false),
      StructField(name = "age", dataType = IntegerType, nullable = false),
      StructField(name = "year", dataType = IntegerType, nullable = false),
      StructField(name = "country", dataType = StringType, nullable = false)
    )
  )
}
 def row(line: List[String]): Row = {
  Row(line(0), line(1), line(2), line(3), line(4).toInt, line(5).toInt,line(6))
}
 val schema = dfSchema(headerColumns)
 val data =
  rdd
    .mapPartitionsWithIndex((index, element) => if (index == 0) element.drop(1) else element) // skip header
    .map(_.split(",").to[List])
    .map(row)
    
   val dataFrame = sqlContext.createDataFrame(data, schema) 
   dataFrame.show
   
   //Problem 1
   //What are the the total number of gold medal winners every year
   val goldmedawinners=dataFrame.select("year","medal_type").filter(dataFrame.col("medal_type")==="gold").groupBy("year").count
   goldmedawinners.show
   
   //Problem 2
   //How many silver medals own by USA in each sport
   val usaSilermedals=dataFrame.select("country","sports","medal_type").filter(dataFrame.col("medal_type")==="silver" && dataFrame.col("country")==="USA").groupBy("sports").count
   usaSilermedals.show
   
}