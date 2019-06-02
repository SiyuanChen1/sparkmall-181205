package com.atguigu.sparkmall.realtime

import java.util

import com.atguigu.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


/**
  * 广告黑名单实时统计
  *
  */
object Req4BlackListApplication {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("x")
    val streamingContext: StreamingContext = new StreamingContext(conf,Seconds(5))

    val topic ="ads_log"

    val kafkaMessage: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,streamingContext)


    val messageDStream: DStream[KafkaMessage] = kafkaMessage.map(datas => {
      val value: String = datas.value()
      val values: Array[String] = value.split(" ")
      KafkaMessage(values(0), values(1), values(2), values(3), values(4))
    })


    val blacklist = "blacklist"
    val filterDStream: DStream[KafkaMessage] = messageDStream.transform(rdd => {
      val client: Jedis = RedisUtil.getJedisClient
      val blacklistSet: util.Set[String] = client.smembers(blacklist)
      client.close()

      val setBroadcast: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(blacklistSet)
      rdd.filter(data => {
        !setBroadcast.value.contains(setBroadcast)
      })

    })
    val dateAndAdvAndUserToClickDStream: DStream[(String, Int)] = filterDStream.map { data => {
      val dateString = DateUtil.parseTimestampToString(data.timestamp.toLong, "yyyy-MM-dd")
      (dateString + "_" + data.adid + "_" + data.userid, 1)
    }
    }
    streamingContext.sparkContext.setCheckpointDir("cp")
    val dateAndAdvAndUserToSumDStream: DStream[(String, Int)] = dateAndAdvAndUserToClickDStream.updateStateByKey {
      case (seq, buffer) => {
        val total = seq.sum + buffer.getOrElse(0)
        Option(total)
      }
    }
    dateAndAdvAndUserToSumDStream.foreachRDD(rdd=>{
      rdd.foreach{
        case(key,sum)=>{
          if (sum >=100){
            val client = RedisUtil.getJedisClient
            client.sadd(blacklist,key.split("_")(2))
            client.close()
          }
        }
      }
    })
    
    



    filterDStream.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
