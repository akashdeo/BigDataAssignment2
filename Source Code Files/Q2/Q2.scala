package com.akash.Q2

import scala.collection._
import org.apache.hadoop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Q2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    val conf = new SparkConf().setAppName("Friends").setMaster("local").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    val input = sc.textFile("C:\\Users\\akash\\Desktop\\Fall 2017\\Big Data\\Homework Questions\\HW2\\q1.txt")

    val result = input.flatMap(pairMap)
      .reduceByKey(pairReduce)
      .filter(!_._2.equals(0)).filter(!_._2.isEmpty)
      .sortByKey()

    val res = result.map(value => (value._1, value._2.size))
      .map(item => item.swap)
      .sortByKey(false, 1)
      .map(item => item.swap)

    val top10 = res.zipWithIndex()
      .filter { case (_, index) => index < 10 }.map { case (value, index) => (value._1, value._2) }

    val userdatafile = sc.textFile("C:\\Users\\akash\\Desktop\\Fall 2017\\Big Data\\Homework Questions\\HW2\\userdata.txt")
    val userdata = userdatafile.flatMap(userMap)

    val top = top10.flatMap {
      value: (String, Int) =>
        val keys = value._1.split(",").toVector
        keys.map((k: String) => (k, value._1 + ":" + value._2.toString))
    }

    val Result = top.join(userdata)

    val finalResult = Result.map {
      value =>
        val k = value._1
        val key = value._2._1
        val vals = value._2._2(0) + "\t" + value._2._2(1) + "\t" + value._2._2(2)
        (key, vals)
    }

    val Output = finalResult.reduceByKey(_ + "\t" + _).map(value => value._1.split(":")(1) + "\t" + value._2)

    Output.saveAsTextFile("C:\\Users\\akash\\Desktop\\Fall 2017\\Big Data\\Homework Questions\\HW2\\output2")

  }

  def pairReduce(first: Set[String], second: Set[String]) = {
      first.toSet intersect second.toSet
  }

  def pairMap(line: String) = {
    val splitline = line.split("\\t+")
    val person = splitline(0)
    val newfriends = if (splitline.length > 1) splitline(1) else "null"
    val nfriends = newfriends.split(",")
    val friends = for (i <- 0 to nfriends.length - 1) yield nfriends(i)
    val pairs = nfriends.map(friend => {
      if (person < friend) person + "," + friend else friend + "," + person
    })
    pairs.map(pair => (pair, friends.toSet))
  }

  def userMap(line: String) = {
    val splitline = line.split(",")
    val data = Array(for (i <- 0 until splitline.length - 1) yield splitline(i))
    data.map(word => (word(0), word.slice(1, word.length - 1)))
  }

}