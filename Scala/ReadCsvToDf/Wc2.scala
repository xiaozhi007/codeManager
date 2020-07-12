package com.mayi.bigdata.wordcount.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Wc2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkConfig").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().appName("sparkMyData").config(conf).getOrCreate()
    import spark.implicits._

    val struct: StructType = StructType(Array(
      StructField("no",IntegerType,nullable = true),
      StructField("age",IntegerType,nullable = true),
      StructField("gender",StringType,nullable = true),
      StructField("occupation",StringType,nullable = true),
      StructField("zipCode",StringType,nullable = true)
    ))//struct

    val df: DataFrame = spark.read.format("csv")
      .option("delimiter","|").schema(struct)
      .load("./data/u.user")
    val ds: Dataset[User] = df.as[User]
    ds.show(5)

  }//main
}//object





