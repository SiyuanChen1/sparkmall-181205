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
object Req4RedisBlackListApplication {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dad")
    val streamingContext: StreamingContext = new StreamingContext(conf,Seconds(5))

    val topic = "ads_log"
    val kafkaMessage: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,streamingContext)

    val messageDStream: DStream[KafkaMessage] = kafkaMessage.map {
      case record => {
        val value: String = record.value()
        val values: Array[String] = value.split(" ")
        KafkaMessage(values(0), values(1), values(2), values(3), values(4))

      }
    }

    val blacklist = "blacklist"

    //messageDStream.print()
    val filterDStream: DStream[KafkaMessage] = messageDStream.transform(rdd => {
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val blacklistSet: util.Set[String] = jedisClient.smembers(blacklist)
      jedisClient.close()

      val setBroadcast: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(blacklistSet)

      rdd.filter(datas => {
        !setBroadcast.value.contains(setBroadcast)
      })

    })

    filterDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(datas=>{
        val client = RedisUtil.getJedisClient
        datas.foreach(message=>{
          val key = "data:adv:user:click"
          val dateString: String = DateUtil.parseTimestampToString(message.timestamp.toLong,"yyyy-MM-dd")
          val field: String = dateString +":"+message.adid+":"+message.userid
          client.hincrBy(key,field,1)

          val sumClick: Long = client.hget(key,field).toLong

          if (sumClick>= 100){
            client.sadd(blacklist,message.userid)
          }



        })





        client.close()

      })
    })







    streamingContext.start()
    streamingContext.awaitTermination()


  }
}

case class KafkaMessage(timestamp: String,province:String,city:String,userid:String,adid:String)
