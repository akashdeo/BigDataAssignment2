package com.akash.Q2

import scala.collection._
import org.apache.hadoop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession

object Q2sql {
     def main(args: Array[String]){
       
     
    System.setProperty("hadoop.home.dir", "C:\\winutil\\"); 
    
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Question 1 Spark-SQL")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()

    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext
    import sqlContext.implicits._

    val data = sc.textFile(args(0))
    val user = sc.textFile(args(1))

    val table = data.flatMap(pairMap).toDF("user1", "user2", "Friends")
    val userdata = user.flatMap(userMap).toDF("ID", "FirstName", "LastName", "Address")

    spark.udf.register("array_intersect", (xs: Seq[String], ys: Seq[String]) => xs.intersect(ys))

    table.registerTempTable("table")

    val result = spark.sql("SELECT DISTINCT t1.user1, t1.user2, size(array_intersect(t1.Friends, t2.Friends))" +
      " as count from table as t1 INNER JOIN table as t2 ON t1.user1 = t2.user1 and t1.user2 = t2.user2 " +
      "where t1.Friends != t2.Friends " +
      "ORDER BY count desc LIMIT 10")

    result.registerTempTable("user")
    userdata.registerTempTable("userdata")
    val userInfo = spark.sql("SELECT t.count, u.FirstName as f1, u.LastName as l1, u.Address as a1, t.user2 " +
      "from user as t INNER JOIN userdata as u ON t.user1 = u.ID")

    userInfo.registerTempTable("userinfo")
    val output = spark.sql("SELECT u.count, u.f1, u.l1, u.a1, user.FirstName, user.LastName, user.Address " +
      "from userinfo as u INNER JOIN userdata as user ON u.user2 = user.ID")

//    output.show()
    val finalOutput = output.rdd.map(_.toString().replace("[", "").replace("]", ""))
      .coalesce(1, true)
    finalOutput.saveAsTextFile(args(2))

    spark.stop()
}
     def pairMap(line: String) = {
    val splitline = line.split("\\t+").toVector
    val person = splitline(0)
    val newfriends = if (splitline.length > 1) splitline(1) else "null"
    val friends = newfriends.split(",").toVector
    val pairs = friends.map(friend => {
      if (person < friend) Vector(person,friend) else Vector(friend, person)
    })
    pairs.map(pair => (pair(0), pair(1), friends.toSet.toSeq))
  }

  def userMap(line: String) = {
    val splitline = line.split(",")
    val data = Array(for (i <- 0 until splitline.length - 1) yield splitline(i))
    data.map(word => (word(0), word(1), word(2), word(3)))
  }
}