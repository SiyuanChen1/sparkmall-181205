package com.atguigu.sparkmall.realtime

import com.atguigu.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis


/**
  * 每天各地区 top3 热门广告
  */
object Req6DateAreaTop3AdvToClickApplication {
  //每天  地区  广告
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("d")
    val streamingContext: StreamingContext = new StreamingContext(conf,Seconds(5))

    val topic = "ads_log"
    val messageDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,streamingContext)
    val actionDStream: DStream[KafkaMessage] = messageDStream.map(data => {
      val value = data.value()
      val values: Array[String] = value.split(" ")

      KafkaMessage(values(0), values(1), values(2), values(3), values(4))


    })
    val fieldAndOneMap: DStream[(String, Int)] = actionDStream.map { action => {
      val dateString: String = DateUtil.parseTimestampToString(action.timestamp.toLong, "yyyy-MM-dd")
      (dateString + ":" + action.province + ":" + action.city + ":" + action.adid, 1)

    }
    }
    streamingContext.sparkContext.setCheckpointDir("cp")
    val fieldAndSum: DStream[(String, Int)] = fieldAndOneMap.updateStateByKey {
      case (seq, buffer) => {
        val total: Int = seq.sum + buffer.getOrElse(0)
        Option(total)
      }
    }
    //每天各地区 top3 热门广告
    //fieldAndSum.print()
    val keyToIdsAndSum: DStream[(String, (String, Int))] = fieldAndSum.map(data => {
      // (dateString + ":" + action.province + ":" + action.city + ":" + action.adid, sum)
      val values: Array[String] = data._1.split(":")
      (values(0) + ":" + values(1), (values(3), data._2))

    })
    //keyToIdsAndSum.print()
    val groupkeyToIdsAndSum: DStream[(String, Iterable[(String, Int)])] = keyToIdsAndSum.groupByKey()
    val result: DStream[(String, List[(String, Int)])] = groupkeyToIdsAndSum.mapValues { data => {
      data.toList.sortWith {
        case (left, right) => {
          left._2 > right._2
        }
      }.take(3)
    }
    }
    result.foreachRDD(rdd=>{
      rdd.foreachPartition(data=>{
        val client: Jedis = RedisUtil.getJedisClient
        val key ="top3:date:pro"



        data.foreach(message=>{
          val values: Array[String] = message._1.split(":")
          val key ="top3_ads_per_day"+values(0)
          val field =values(1)
          import  org.json4s.JsonDSL._
          val listString: String = JsonMethods.compact(JsonMethods.render(message._2))

          client.hset(key,field,listString)
        })

        client.close()
      })
    })


    streamingContext.start()
    streamingContext.awaitTermination()


  }
}
