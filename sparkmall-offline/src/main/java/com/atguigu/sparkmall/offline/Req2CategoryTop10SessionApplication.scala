package com.atguigu.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.UUID

import com.atguigu.sparkmall.common.model.UserVisitAction
import com.atguigu.sparkmall.common.util.ConfigurationUtil
import org.apache.commons.configuration.ConfigurationFactory.ConfigurationBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Req2CategoryTop10SessionApplication {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("chensy")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._

    spark.sql("use "+ConfigurationUtil.getValueFromConfig("hive.database"))

    val startDate = ConfigurationUtil.getValueFromCondition("startDate")
    val  endDate: String = ConfigurationUtil.getValueFromCondition("endDate")

    var sql = "select * from user_visit_action where 1=1 "
    if (startDate != null){
      sql  = sql +" and action_time >= '" + startDate + "'"
    }
    if(endDate != null){
      sql =  sql +" and action_time <= '" + endDate + "'"
    }


    val dataFrame: DataFrame = spark.sql(sql)
    val dataSet: Dataset[UserVisitAction] = dataFrame.as[UserVisitAction]
    val acctionRdd: RDD[UserVisitAction] = dataSet.rdd
    //println(acctionRdd.collect().take(5).mkString(","))
    //println(acctionRdd.count())
    //TODO 需求2  Top10 热门品类中 Top10 活跃 Session 统计

    val driverClass: String = ConfigurationUtil.getValueFromConfig("jdbc.driver.class")
    val url: String = ConfigurationUtil.getValueFromConfig("jdbc.url")
    val user: String = ConfigurationUtil.getValueFromConfig("jdbc.user")
    val password: String = ConfigurationUtil.getValueFromConfig("jdbc.password")

    Class.forName(driverClass)

    val connection: Connection = DriverManager.getConnection(url,user,password)
    //connection.prepareStatement("use sparkmall181205").execute()

    val statement: PreparedStatement = connection.prepareStatement("select category_id from category_top10 ")
    val set: ResultSet = statement.executeQuery()



    val arr1 : ArrayBuffer[Long] = new ArrayBuffer[Long]()
    while( set.next()){
       arr1 +=set.getLong(1)
    }
    //println(arr1.mkString(","))
    statement.close()
    connection.close()



    val ids: List[Long] = arr1.toList
    //println(ids)

    // TODO Top10 热门品类中 Top10 活跃 Session 统计
    val actionFliterRDD: RDD[UserVisitAction] = acctionRdd.filter(action => {
      ids.contains(action.click_category_id)
    })
    //println(actionFliterRDD.count())
    val acctionReduce: RDD[(String, Int)] = actionFliterRDD.map(data => {
      (data.click_category_id + "_" + data.session_id, 1)
    }).reduceByKey(_ + _)
    val acctionCateoryToSessionAndSum: RDD[(String, (String, Int))] = acctionReduce.map {
      case (ids, sum) => {
        val k = ids.split("_")
        (k(0), (k(1), sum))
      }
    }
    val groupRDD1: RDD[(String, Iterable[(String, Int)])] = acctionCateoryToSessionAndSum.groupByKey()
    val sortRDD: RDD[(String, List[(String, Int)])] = groupRDD1.mapValues(data => {
      data.toList.sortWith {
        case (left, right) => {
          left._2 > right._2
        }
      }.take(10)
    })
    val task: String = UUID.randomUUID().toString
    val mapValue1: RDD[CategoryTop10Session10] = sortRDD.map(datas => {
      datas._2.map {
        list => {
          CategoryTop10Session10(task, datas._1, list._1, list._2.toLong)
        }
      }

    }).flatMap(x => x)





    mapValue1.foreachPartition { datas => {
      val driverClass: String = ConfigurationUtil.getValueFromConfig("jdbc.driver.class")
      val url: String = ConfigurationUtil.getValueFromConfig("jdbc.url")
      val user: String = ConfigurationUtil.getValueFromConfig("jdbc.user")
      val password: String = ConfigurationUtil.getValueFromConfig("jdbc.password")
      Class.forName(driverClass)

      val connection: Connection = DriverManager.getConnection(url, user, password)
      val statement: PreparedStatement = connection.prepareStatement("insert into category_top10_session_count values (?,?,?,?)")
      datas.foreach(action=>{

        statement.setString(1,action.task)
        statement.setString(2,action.category)
        statement.setString(3,action.sessionId)
        statement.setLong(4,action.click)
        statement.executeUpdate()
      })
      statement.close()
      connection.close()
      }



    }

    spark.close()



  }

}
case class CategoryTop10Session10(task:String,category:String,sessionId:String,click:Long){}
