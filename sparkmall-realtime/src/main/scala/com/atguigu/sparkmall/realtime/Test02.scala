package com.atguigu.sparkmall.realtime

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Test02 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SiyuanChen")
    val sc: SparkContext = new SparkContext(conf)


    val dFile: RDD[String] = sc.textFile("d")
    //dFile.foreach(println)
    val datas: RDD[T] = dFile.map(data => {
      val splits: Array[String] = data.split(" ")
      T(splits(0), splits(1))

    })
    //每个店铺访问次数 top3的访客信息， 输出店铺名称、访客id、访问次数
    //datas.foreach(println)
    val shopAndUserToOne: RDD[(String, Int)] = datas.map(action => {
      (action.shop + "_" + action.user, 1)
    })
    val shopAndUserToSum: RDD[(String, Int)] = shopAndUserToOne.reduceByKey(_+_)

    //shopAndUserToSum.foreach(println)
    val shopToUserAndSum: RDD[(String, (String, Int))] = shopAndUserToSum.map(action => {
      val splits: Array[String] = action._1.split("_")
      (splits(0), (splits(1), action._2))
    })
    //shopToUserAndSum.foreach(println)
    val groupShopToUserAndSum: RDD[(String, Iterable[(String, Int)])] = shopToUserAndSum.groupByKey()
    //groupShopToUserAndSum.foreach(println)
    val sortN: RDD[(String, List[(String, Int)])] = groupShopToUserAndSum.mapValues(action => {
      action.toList.sortWith {
        case (left, right) => {
          left._2 > right._2
        }

      }.take(3)
    })
    sortN.foreach(println)











  }

}


