package com.atguigu.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement, Statement}
import java.util.UUID

import com.atguigu.sparkmall.common.model.UserVisitAction
import com.atguigu.sparkmall.common.util.ConfigurationUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


//TODO 获取点击、下单和支付数量排名前 10 的品类
object Req1CategoryTop10Application {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Siyuanc")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    val startDate: String = ConfigurationUtil.getValueFromCondition("startDate")
    val endDate: String = ConfigurationUtil.getValueFromCondition("endDate")



    var sql = "select * from user_visit_action where 1=1 "
    if (startDate != null){
      sql  = sql + " and action_time >= ' "+ startDate +" '"
    }

    if (endDate  !=  null){
      sql =sql +" and action_time <= '"+ endDate +"'"
    }

    spark.sql(" use "+ConfigurationUtil.getValueFromConfig("hive.database"))

    val dataFrame: DataFrame = spark.sql(sql)
    // println(dataFrame)
    val dataSet: Dataset[UserVisitAction] = dataFrame.as[UserVisitAction]
    val actionRdd: RDD[UserVisitAction] = dataSet.rdd
   //println(actionRdd.collect().take(5).mkString(","))

    //TODO 需求1
    val accumulator: CategoryCountAccumulator = new CategoryCountAccumulator
    spark.sparkContext.register(accumulator)

    actionRdd.foreachPartition(datas=>{
      datas.foreach(action=>{
        if(action.click_category_id != -1){
          accumulator.add(action.click_category_id+"_click")
        }else {
          if (action.order_category_ids != null) {
            val ids: Array[String] = action.order_category_ids.split(",")
            for (elem <- ids) {
              accumulator.add(elem + "_order")
            }
          }else{
            if (action.pay_category_ids != null) {
              val ids: Array[String] = action.pay_category_ids.split(",")
              for (elem <- ids) {
                accumulator.add(elem + "_pay")
              }
            }
          }
        }
      })
    })
    val accValue: mutable.HashMap[String, Long] = accumulator.value
    //accValue.foreach(println)

    val groupMap: Map[String, mutable.HashMap[String, Long]] = accValue.groupBy(datas => {
      datas._1.split("_")(0)
    })
    //groupMap.foreach(println)
      val taskId: String = UUID.randomUUID().toString
    val objectCategory = groupMap.map {
      case (catory, map) => {
        CategoryTop10(taskId, catory, map.getOrElse(catory + "_click", 0), map.getOrElse(catory + "_order", 0), map.getOrElse(catory + "_pay", 0))
      }
    }
    //objectCategory.foreach(println)


    val sortObj: List[CategoryTop10] = objectCategory.toList.sortWith {
      case (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }

        } else {
          false
        }
      }
    }

    //println(sortObj.take(10))

    val top10s: List[CategoryTop10] = sortObj.take(10)

    val driverClass: String = ConfigurationUtil.getValueFromConfig("jdbc.driver.class")
    val url: String = ConfigurationUtil.getValueFromConfig("jdbc.url")
    val user: String = ConfigurationUtil.getValueFromConfig("jdbc.user")
    val password: String = ConfigurationUtil.getValueFromConfig("jdbc.password")

    Class.forName(driverClass)
    val connection: Connection = DriverManager.getConnection(url,user,password)
    val statement: PreparedStatement = connection.prepareStatement("insert into category_top10 values( ?, ?, ?, ?, ? )")
    top10s.foreach(data=>{
      statement.setString(1,data.task)
      statement.setString(2,data.categoryId)
      statement.setLong(3,data.clickCount)
      statement.setLong(4,data.orderCount)
      statement.setLong(5,data.payCount)
      statement.executeUpdate()

    })
    statement.close()
    connection.close()
    spark.close()
    /*
    // TODO 4.5 获取前10名的数据
    val top10s: List[CategoryTop10] = sortObj.take(10)

    //top10s.foreach(println)

    // TODO 4.6 将统计结果保存到Mysql中
    val driverClass = ConfigurationUtil.getValueFromConfig("jdbc.driver.class")
    val url = ConfigurationUtil.getValueFromConfig("jdbc.url")
    val user = ConfigurationUtil.getValueFromConfig("jdbc.user")
    val password = ConfigurationUtil.getValueFromConfig("jdbc.password")

    Class.forName(driverClass)

    val connection: Connection = DriverManager.getConnection(url, user, password)
    val statement: PreparedStatement = connection.prepareStatement("insert into category_top10 values ( ?, ?, ?, ?, ? )")

    top10s.foreach(data=>{
      statement.setString(1, data.task)
      statement.setString(2, data.categoryId)
      statement.setLong(3, data.clickCount)
      statement.setLong(4, data.orderCount)
      statement.setLong(5, data.payCount)
      statement.executeUpdate()
    })

    statement.close()
    connection.close()

    // 释放资源
    spark.stop()

    */



  }
}

class CategoryCountAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]]{

  var map = new mutable.HashMap[String,Long]()


  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new CategoryCountAccumulator
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(v: String): Unit = {
      map(v) =  map.getOrElse(v,0L) +1
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    var  map1 = map
    var map2 = other.value
    map = map1.foldLeft(map2){
      case (tempMap , (k,sumCount)) =>{
        tempMap(k) = tempMap.getOrElse(k,0L)+sumCount
        tempMap
      }
    }

  }

  override def value: mutable.HashMap[String, Long] = map
}

case class CategoryTop10(task:String,categoryId:String,clickCount:Long,orderCount:Long,payCount:Long){}