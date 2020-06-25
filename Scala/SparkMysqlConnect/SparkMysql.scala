package com.mayi.bigdata.wordcount.spark

import org.apache.spark._
import org.apache.spark.rdd.JdbcRDD


object WordCount {
  def main(args: Array[String]): Unit = {
    println("就绪！！！")
    //创建spark上下文对象
    val sc=new SparkContext("local[*]","wordcount")

    val driver="com.mysql.jdbc.Driver"
    val url="jdbc:mysql://192.168.2.16:3306/test"
    val userName="root"
    val passwd="meimima120"

    val sql="select name,age from user where id >=? and id <=?"
    // 查询操作
    val value1= new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        java.sql.DriverManager.getConnection(url, userName, passwd)
      },
      sql,
      0,
      2,
      2,
      // 对返回值进行操作
      (rs)=>(rs.getString(1),rs.getInt(2))
    )
    value1.foreach(println)
    val data=sc.makeRDD(List(("zhang1",20),("zhang2",30),("zhang3",40)))
    // 使用foreachPartition减少连接数
    data.foreachPartition { data => {
      val conn = java.sql.DriverManager.getConnection(url, userName, passwd)
      data.foreach {
        case (name, age) => {
          Class.forName("com.mysql.jdbc.Driver").newInstance()
          val ps = conn.prepareStatement("insert into user(name,age) value (?,?)")
          ps.setString(1, name)
          ps.setInt(2, age)
          ps.executeUpdate()
          ps.close()

      }
      conn.close()
    }
    }

    }

}}
