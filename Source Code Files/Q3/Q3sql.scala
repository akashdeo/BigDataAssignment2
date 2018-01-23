package com.akash.Q3

import scala.collection._
import org.apache.hadoop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._
import org.apache.spark.sql.SparkSession

object Q3sql {
//     
def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Question 3 Spark-SQL")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()

    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext
    import sqlContext.implicits._

    val review = sc.textFile(args(0))
    val business = sc.textFile(args(1))

    val reviewData = review.map(reviewMap)
      .toDF("BusinessID", "Rating")
    val businessData = business.map(businessMap).distinct()
      .toDF("BusinessID", "Address", "Categories")

    businessData.registerTempTable("business")
    reviewData.registerTempTable("review")

    val Result = spark.sql("SELECT DISTINCT b.BusinessID, b.Address, b.Categories, count(r.Rating) as NumberRated " +
      "from review as r INNER JOIN business as b ON r.BusinessID = b.BusinessID " +
      "GROUP BY b.BusinessID, b.Address, b.Categories " +
      "ORDER BY NumberRated desc LIMIT 10")

    val Output = Result.rdd.map(_.toString().replace("[", "").replace("]", ""))
      .coalesce(1, true)
    Output.saveAsTextFile(args(2))

    spark.stop()

  }

  def reviewMap(line: String) = {
    val data = line.split("::").toVector
    (data(2), data(3).toDouble)
  }

  def businessMap(line: String) = {
    val data = line.split("::").toVector
    (data(0), data(1), data(2))
  }
}