package com.atguigu.sparkmall.realtime

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Test01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SiyuanChen")
    val sc: SparkContext = new SparkContext(conf)


    val dFile: RDD[String] = sc.textFile("d")
    //dFile.foreach(println)
    val tUser: RDD[T] = dFile.map(data => {
      val splits: Array[String] = data.split(" ")
      T(splits(0), splits(1))

    })
    val tMap: RDD[(String, (String, Int))] = tUser.map(data => {
      (data.shop, (data.user, 1))
    })
    val tGroup: RDD[(String, Iterable[(String, Int)])] = tMap.groupByKey()
    val uMapValue: RDD[(String, List[(String, Int)])] = tGroup.mapValues(data => {
      val list: List[(String, Int)] = data.toList
      list.distinct

      //println(list)
    })
    //uMapValue.foreach(println)
    val unit: RDD[(String, Int)] = uMapValue.map(data => {
      (data._1, data._2.size)
    })
    unit.foreach(println)



  }

}

case class T(user:String,shop:String){}
