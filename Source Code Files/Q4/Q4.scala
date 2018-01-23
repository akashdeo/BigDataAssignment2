package com.akash.Q4

import scala.collection._
import org.apache.hadoop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
object Q4 {
    def main(args: Array[String]) {
         System.setProperty("hadoop.home.dir", "C:\\winutil\\");
         val conf = new SparkConf().setAppName("Question3").setMaster("local[2]").set("spark.executor.memory","1g");
         val sc = new SparkContext(conf)
         val business = args(0)
         val review = args(1)
         val fileBusiness = sc.textFile(business)
         val fileReview = sc.textFile(review)
         val ReviewRDD_split = fileReview.map(line => line.split("::")).map(tokens => ( tokens(1), tokens(2) , tokens(3) )).map(x => (x._2,List(x._1,x._3)))   
         val BusinessRDD_split = fileBusiness.map(line => line.split("::")).map(tokens => ( tokens(0), tokens(1)  )).map(x => (x._1,List(x._2)))
         val result = ReviewRDD_split.join(BusinessRDD_split).distinct()
         val resultFinal = result.filter{case (x) => x._2._2(0).toString().contains("Palo Alto")}.map(x => x._2._1(0)+ "\t" + x._2._1(1))
         resultFinal.saveAsTextFile(args(2))
  }
}