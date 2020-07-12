package com.mayi.bigdata.wordcount.spark

import org.apache.spark._
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}




object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkConfig").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().appName("sparkMyData").config(conf).getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.format("csv")
      .option("header","false")
      .option("delimiter","|")
      .option("inferSchema",true.toString)
      .load("./data/u.user")
    val df1: DataFrame =df.toDF("no","age","gender","occupation","zipCode")
    val ds: Dataset[User] = df1.as[User]
    ds.show(5)



}//main
}//object

case class User(no:Int,age:Int,gender:String,occupation:String,zipCode:String)