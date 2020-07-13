package com.mayi.bigdata.wordcount.spark


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object Item {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Item")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().appName("ItemData").config(conf).getOrCreate()
    import spark.implicits._
    val struct: StructType = StructType(Array(
      StructField("id", dataType = StringType, nullable = true),
      StructField("name", dataType = StringType, nullable = true),
      StructField("date", dataType = StringType, nullable = true),
      StructField("null", dataType = StringType, nullable = true)
    ))//struct
    val df: DataFrame = spark.read.format("csv").option("delimiter", "|").schema(struct).load("./data/u.item")
//    df.show(5)
    df.createOrReplaceTempView("item")
//    spark.sql("select substring(date,8,4) year from item limit 10").show()
    spark.udf.register("catYear",catYear _)
//    spark.sql("select year1,count(*) from (select substring(date,8,4) year1 from item) t group by year1 ").show()
    val dfYear: DataFrame = spark.sql("select catYear(date) year1 from item")
    dfYear.show()
    dfYear.createOrReplaceTempView("dfYear1")
    spark.sql("select year1,count(year1) count from dfYear1 group by year1 order by count").show()

  }//main




  /*获取日期年份部分，注册为spark UDF 函数*/
  //自定义函数要考虑周全，注意捕捉异常，否则会造成运行失败
  def catYear(s:String):Int={
    try{    s.takeRight(4).toInt
    }catch {
      case e:Exception => println("发现一个异常值###############")
        return 1900
    }

  }

}//object









