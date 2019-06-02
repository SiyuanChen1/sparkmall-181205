package com.atguigu.sparkmall.offline

import com.atguigu.sparkmall.common.model.UserVisitAction
import com.atguigu.sparkmall.common.util.ConfigurationUtil
import org.apache.ivy.core.module.descriptor.ConfigurationGroup
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Req3PageflowApplication {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("che")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._

    spark.sql("use "+ConfigurationUtil.getValueFromConfig("hive.database"))

    val startDate = ConfigurationUtil.getValueFromCondition("startDate")
    val endDate: String = ConfigurationUtil.getValueFromCondition("endDate")

    var sql = "select * from user_visit_action where 1=1 "
    if (startDate != null){
      sql = sql +" and action_time >= '" +startDate+ "'"
    }
    if (endDate != null){
      sql = sql +" and action_time <= '" +endDate+ "'"
    }
    val dataFrame: DataFrame = spark.sql(sql)
    val dataSet: Dataset[UserVisitAction] = dataFrame.as[UserVisitAction]
    val actionRdd: RDD[UserVisitAction] = dataSet.rdd
    //println(actionRdd.count())

    //TODO 页面单跳转化率统计
    //TODO 计算分母
    val targetPageFlows: String = ConfigurationUtil.getValueFromCondition("targetPageFlow")
    val pages: Array[String] = targetPageFlows.split(",")
    val pageidFilterRDD: RDD[UserVisitAction] = actionRdd.filter(data => {
      //val pages: Array[String] = targetPageFlows.split(",")
      pages.contains(data.page_id.toString)
    })
   // println(pageidFilterRDD.count())

    val mapReduceBy: RDD[(Long, Int)] = pageidFilterRDD.map { datas => {
      (datas.page_id, 1)
    }
    }.reduceByKey(_ + _)
    val map12: Map[Long, Int] = mapReduceBy.collect().toMap

    //println(mapReduceBy.collect().take(10).mkString(","))


    // TODO 计算分子
    val groupSession: RDD[(String, Iterable[UserVisitAction])] = actionRdd.groupBy(datas=>{datas.session_id})
    val mapZipSort: RDD[(String, List[(Long, Long)])] = groupSession.mapValues(datas => {
      val mapSortTime: List[UserVisitAction] = datas.toList.sortWith {
        (left, right) => {
          left.action_time < right.action_time
        }
      }
      val longs: List[Long] = mapSortTime.map(actions => {
        (actions.page_id)
      })

      val tuples: List[(Long, Long)] = longs.zip(longs.tail)
      tuples


    })
    val mapListZip: RDD[List[(Long, Long)]] = mapZipSort.map(_._2)
    //println(mapListZip.collect().take(3).mkString(","))
    val mapListZipFlat: RDD[(Long, Long)] = mapListZip.flatMap(x=>x)

    val pagesZip: Array[(String, String)] = pages.zip(pages.tail)
    val pagesZipList: Array[String] = pagesZip.map(data => {
      (data._1 + "_" + data._2)
    })


    val Elt: RDD[(Long, Long)] = mapListZipFlat.filter(datas => {
      pagesZipList.contains(datas._1 + "_" + datas._2)
    })
    //println(Elt.collect().take(5).mkString(","))
    val mapvalue1: RDD[(String, Int)] = Elt.map(data => {
      (data._1 + "_" + data._2, 1)
    })
    val reduceMap: RDD[(String, Int)] = mapvalue1.reduceByKey(_+_)
    //println(reduceMap.collect().take(5).mkString(","))

    reduceMap.foreach{
      case(pagess,sum)=>{
        val str: String = pagess.split("_")(0)
        println(pagess +" = " +sum.toDouble/map12(str.toLong))
       // println( pids + "=" + sum.toDouble/pidToSumMap(pidA.toLong) )
      }
    }








    spark.close()

  }

}
