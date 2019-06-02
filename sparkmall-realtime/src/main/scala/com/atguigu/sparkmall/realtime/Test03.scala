package com.atguigu.sparkmall.realtime

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test03 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("s")
    val sc: SparkContext = new SparkContext(conf)

    val datas: RDD[String] = sc.textFile("e")
    val actionRdd: RDD[User_age] = datas.map(datas => {
      val splits: Array[String] = datas.split(",")
      User_age(splits(0), splits(1), splits(2).toInt)
    })
    //actionRdd.foreach(println)

    // TODO 所有用户和活跃用户的总数和平均年龄
    /**
      * 所用用户的总数和平均年龄
      */
    val IdToAge: RDD[(String, Iterable[Int])] = actionRdd.map(data => {
      (data.user_id, data.age)
    }).groupByKey()
    //IdToAge.foreach(println)
    val userIdToAge: RDD[(String, Int)] = IdToAge.mapValues(action => {
      action.max
    })
    //userIdToAge.foreach(println)

    val userCount: Long = userIdToAge.count()


    val value: RDD[(Int, Int)] = userIdToAge.map(action => {
      (1, action._2)
    }).reduceByKey(_ + _)
    value.foreach(action=>{
      val avgAge: Double = (action._2/userCount).toDouble
      println("所有用户："+userCount+","+"平均年龄"+avgAge)
    })

    //TODO 活跃用户的总数和平均年龄

    //actionRdd.foreach(println)

    val actionRDDNew: RDD[User_age] = actionRdd.distinct()
    //actionRDDNew.foreach(println)
    val userIGroup: RDD[(String, Iterable[User_age])] = actionRDDNew.groupBy(data=>data.user_id)
    //userIGroup.foreach(println)
    val sortRDD: RDD[(String, List[(String, Int)])] = userIGroup.mapValues(action => {
      val list: List[User_age] = action.toList
      list.map(actions => {
        (actions.date, actions.age)
      }).sortWith {
        case (left, right) => {
          left._1 < right._1
        }

      }

    })
    val sortRDDS: RDD[(String, List[String])] = sortRDD.mapValues(action => {
      action.map(data => data._1)
    })


    val sortZip: RDD[(String, List[(String, String)])] = sortRDDS.mapValues(actions => {
      val tuples: List[(String, String)] = actions.zip(actions.tail)
      tuples
    })

    //sortZip.foreach(println)
    val result: RDD[(String, List[(Int, Int)])] = sortZip.map(action => {
      val strList: List[(String, String)] = action._2
      val tuples: List[(Int, Int)] = strList.map(ac => {
        val i1: Int = ac._1.substring(8).toInt
        val i2: Int = ac._2.substring(8).toInt
        (i1, i2)
      })
      val tupFileter: List[(Int, Int)] = tuples.filter(acc => {
        acc._2 - acc._1 == 1
      })
      (action._1, tupFileter)


    })
    //result.foreach(println)
    val unit: RDD[(String, List[(Int, Int)])] = result.filter(acc=>acc._2.size !=0)
    unit.foreach(println)









  }
}
case class User_age(date:String,user_id:String,age:Int){}
