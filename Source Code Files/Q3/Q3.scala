package com.akash.Q3

import scala.collection._
import org.apache.hadoop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Ques3 {
     def main(args: Array[String]) {
         System.setProperty("hadoop.home.dir", "C:\\winutil\\");
         val conf = new SparkConf().setAppName("Question3").setMaster("local[2]").set("spark.executor.memory","1g");
         val sc = new SparkContext(conf)
         val business = "C:\\Users\\akash\\Desktop\\Fall 2017\\Big Data\\Homework Questions\\HW2\\business.csv"
         val review = "C:\\Users\\akash\\Desktop\\Fall 2017\\Big Data\\Homework Questions\\HW2\\review.csv"
         val fileBusiness = sc.textFile(business)
         val fileReview = sc.textFile(review)
         var ReviewRDD_split = fileReview.map(line => line.split("::")).map(tokens => ( tokens(2) , tokens(3) )).groupBy(x => x._1).mapValues(_.toList).map(x => (x._1,x._2.size))  
         var BusinessRDD_split = fileBusiness.map(line => line.split("::")).map(tokens => ( tokens(0), tokens(1) , tokens(2) )).map(x => (x._1,List(x._2,x._3)))
         val result = ReviewRDD_split.join(BusinessRDD_split).distinct().sortBy(x => x._2._1,false)
         val resultFinal = result.zipWithIndex.filter{case (_,index) => index<10}.keys.map( x => x._1 +"\t"+ x._2._2(0) +"\t" + x._2._2(1) + "\t" + x._2._1)
         resultFinal.saveAsTextFile("C:\\Users\\akash\\Desktop\\Fall 2017\\Big Data\\Homework Questions\\HW2\\output3")
         
  }
}
