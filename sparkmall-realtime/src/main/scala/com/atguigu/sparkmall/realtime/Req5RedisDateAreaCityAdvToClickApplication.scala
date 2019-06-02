package com.atguigu.sparkmall.realtime

import com.atguigu.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  *
  * 每天各地区各城市各广告的点击流量实时统计
  */
object Req5RedisDateAreaCityAdvToClickApplication {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("x")
    val streamingContext: StreamingContext = new StreamingContext(conf,Seconds(5))

    val topic ="ads_log"
    val kafkaMessage: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,streamingContext)
    val messageDStream: DStream[KafkaMessage] = kafkaMessage.map {
      datas => {
        val value: String = datas.value()
        val values: Array[String] = value.split(" ")
        KafkaMessage(values(0), values(1), values(2), values(3), values(4))

      }
    }
    //每天各地区各城市各广告
    messageDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(data=>{
        val client: Jedis = RedisUtil.getJedisClient

        val key ="date:pro:city:ads"
        data.foreach(message=>{
          val dateString: String = DateUtil.parseTimestampToString(message.timestamp.toLong)

          val field = dateString+":"+message.province+":"+message.city+":"+message.adid
          client.hincrBy(key,field,1)
        })
        client.close()
      })
    })


    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
