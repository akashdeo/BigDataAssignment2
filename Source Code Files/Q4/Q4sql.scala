package com.akash.Q4

import scala.collection._
import org.apache.hadoop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._
import scala.reflect.api.materializeTypeTag
import org.apache.spark.sql.SparkSession

object Q4sql {
//    def main(args: Array[String]) {
//         val conf = new SparkConf().setAppName("Question4Scala").setMaster("local[2]").set("spark.executor.memory","1g");
//         val sc = new SparkContext(conf)
//         sc.setLogLevel("WARN")
//         val business = "C:\\Users\\akash\\Desktop\\Fall 2017\\Big Data\\Homework Questions\\HW2\\business.csv"
//         val review = "C:\\Users\\akash\\Desktop\\Fall 2017\\Big Data\\Homework Questions\\HW2\\review.csv"
//         val fileBusiness = sc.textFile(business)
//         val fileReview = sc.textFile(review)
//         val ReviewRDD_split = fileReview.map(line => line.split("::")).map(tokens => ( tokens(1), tokens(2) , tokens(3) )).map(x => (x._2,x._1,x._3))   
//         val BusinessRDD_split = fileBusiness.map(line => line.split("::")).map(tokens => ( tokens(0), tokens(1)  )).map(x => (x._1,x._2))
//         val sqlContext= new org.apache.spark.sql.SQLContext(sc)
//         import sqlContext.implicits._
//         val table1 = ReviewRDD_split.toDF("id","user_id","ratings")
//         val table2 = BusinessRDD_split.toDF("id","address")
//         val intermediateTable = table1.as("t1").join(table2.as("t2"),Seq("id")).distinct()
//         val intermediateResult = intermediateTable.filter(table2("address").contains("Palo Alto") ).select(intermediateTable("user_id"), table1("ratings")).show()
//         
//    }
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Question 4 Spark-SQL")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()

    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext
    import sqlContext.implicits._

    val review = sc.textFile(args(0))
    val business = sc.textFile(args(1))

    val reviewData = review.map(reviewMap)
      .toDF("BusinessID", "Rating", "UserID")
    val businessData = business.map(businessMap).distinct()
      .toDF("BusinessID", "Address")

    businessData.registerTempTable("business")
    reviewData.registerTempTable("review")

    val Result = spark.sql("SELECT DISTINCT r.UserID, r.Rating, b.BusinessID " +
      "from review as r INNER JOIN business as b ON r.BusinessID = b.BusinessID " +
      "where b.Address like '%Palo Alto%' ").select("UserID", "Rating")

    val Output = Result.rdd.map(_.toString().replace("[", "").replace("]", ""))
      .coalesce(1, true)
    Output.saveAsTextFile(args(2))

  }

  def reviewMap(line: String) = {
    val data = line.split("::")
    (data(2), data(3), data(1))
  }

  def businessMap(line: String) = {
    val data = line.split("::").toVector
    (data(0), data(1))
  }
}