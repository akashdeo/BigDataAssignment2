package com.akash.Q1

import scala.collection._
import org.apache.hadoop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Q1 {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    val conf = new SparkConf().setAppName("Spark Friends").setMaster("local").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    val fileFriends = sc.textFile("C:\\Users\\akash\\Desktop\\Fall 2017\\Big Data\\Homework Questions\\HW2\\q1.txt")
    val result = fileFriends.flatMap(Map)
      .reduceByKey(intersection)
      .filter(!_._2.equals("null")).filter(!_._2.isEmpty)
      .sortByKey()
    val res = result.map(value => value._1 + "\t" + value._2.size)
    //res.foreach(println)
    res.saveAsTextFile("C:\\Users\\akash\\Desktop\\Fall 2017\\Big Data\\Homework Questions\\HW2\\output1")
  }

  def intersection(first: Set[String], second: Set[String]) = {
    first.toSet intersect second.toSet
  }

  def Map(line: String) = {
    val line1 = line.split("\\t+")
    val person = line1(0)
    val newfriends = if (line1.length > 1) line1(1) else "null"
    val nfriends = newfriends.split(",")
    val friends = for (i <- 0 to nfriends.length - 1) yield nfriends(i)
    val pairs = nfriends.map(friend => {
      if (person < friend) person + "," + friend else friend + "," + person
    })
    pairs.map(pair => (pair, friends.toSet))
  
      
  }
}