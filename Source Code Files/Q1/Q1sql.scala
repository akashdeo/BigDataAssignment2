package com.akash.Q1

import scala.collection._
import org.apache.hadoop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession

object Q1sql {
//  def main(args: Array[String]) {
//    System.setProperty("hadoop.home.dir", "C:\\winutil\\");
//    val conf = new SparkConf().setAppName("Spark Friends").setMaster("local").set("spark.executor.memory", "1g")
//    val sc = new SparkContext(conf)
//    val spark = SparkSession.builder()
//                            .appName("Spark Friends")
//                            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
//                            .getOrCreate()
//    
//    
//    import spark.implicits._
//    val fileFriends = sc.textFile("C:\\Users\\akash\\Desktop\\Fall 2017\\Big Data\\Homework Questions\\HW2\\q1.txt")
//    val result = fileFriends.flatMap(Map)  
//    val res = result.map(value => (value._1.split(",")(0),value._1.split(",")(1),value._2.toList))
//    val table1 = res.toDF("f1","f2","list").distinct()
//    val table2 = res.toDF("f1","f2","list").distinct()
//    //val table3 = table1.as("t1").join(table2.as("t2"), table1("f1") === table2("f1") and table1("f2") === table2("f2"))
//    //val table4 = table3.filter(table1("list") !== table2("list"))
//    //table4.show()
//    
//    
//    spark.udf.register("array_intersect", 
//       (xs: Seq[String], ys: Seq[String]) => xs.intersect(ys))
//    val table5 = spark.sql("SELECT t1.f1 as user1, t1.f2 as user2, size(array_intersect(t1.list, t2.list)) as count " +
//      "from table1 as t1 INNER JOIN table1 as t2 ON t1.f1 = t2.f1 and t1.f2 = t2.f2 where t1.list != t2.list").distinct()
//    table5.show()
//    
//  }
//    
//    def intersection(first: Set[String], second: Set[String]) = {
//      first.toSet intersect second.toSet
//    } 
//    
//    def Map(line: String) = {
//      val line1 = line.split("\\t+")
//      val person = line1(0)
//      val newfriends = if (line1.length > 1) line1(1) else "null"
//      val nfriends = newfriends.split(",")
//      val friends = for (i <- 0 to nfriends.length - 1) yield nfriends(i)
//      val pairs = nfriends.map(friend => {
//      if (person < friend) person + "," + friend else friend + "," + person
//      })
//      pairs.map(pair => (pair, friends))
//    }
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Question 1")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()

    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext
    import sqlContext.implicits._

    val data = sc.textFile(args(0))

    val table = data.flatMap(pairMap).toDF("friendpair", "Friends")

    spark.udf.register("array_intersect", (xs: Seq[String], ys: Seq[String]) => xs.intersect(ys))

    table.registerTempTable("table")

    val Result = spark.sql("SELECT t1.friendpair as users, size(array_intersect(t1.Friends, t2.Friends)) as count " +
      "from table as t1 INNER JOIN table as t2 ON t1.friendpair = t2.friendpair where t1.Friends != t2.Friends").distinct()

    val Output = Result.rdd.map(_.toString().replace("[", "").replace("]", ""))
      .coalesce(1, true)

    Output.saveAsTextFile(args(1))

    spark.stop()

  }

  def pairMap(line: String) = {
    val splitline = line.split("\\t+").toVector
    val person = splitline(0)
    val newfriends = if (splitline.length > 1) splitline(1) else "null"
    val friends = newfriends.split(",").toVector
    val pairs = friends.map(friend => {
      if (person < friend) person + "," + friend else friend + "," + person
    })
    pairs.map(pair => (pair, friends.toSet.toSeq))
  }
}