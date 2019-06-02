package com.atguigu.sparkmall.realtime

import com.atguigu.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


/**
  *
  * 每天各地区 各城市广告的点击流实时统计
  *
  * 每天   地区   城市   广告
  */
object Req5DateAreaCityAdvToClickApplication {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("x")
    val streamingContext: StreamingContext = new StreamingContext(conf,Seconds(5))


    val topic = "ads_log"
    val messageDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,streamingContext)
    val actionDStream: DStream[KafkaMessage] = messageDStream.map(data => {
      val value: String = data.value()
      val values = value.split(" ")
      KafkaMessage(values(0), values(1), values(2), values(3), values(4))

    })
    //actionDStream.print()
    //每天   地区   城市   广告
    val keyAndOneMap: DStream[(String, Int)] = actionDStream.map(action => {

      val dateString: String = DateUtil.parseTimestampToString(action.timestamp.toLong, "yyyy-MM-dd")
      (dateString + ":" + action.province + ":" + action.city + ":" + action.adid, 1)

    })
    streamingContext.sparkContext.setCheckpointDir("cp")

    val updateSum: DStream[(String, Int)] = keyAndOneMap.updateStateByKey {
      case (seq, buffer) => {
        val total: Int = seq.sum + buffer.getOrElse(0)
        Option(total)

      }
    }
    updateSum.foreachRDD(rdd=>{
      rdd.foreachPartition(message=>{
        val client: Jedis = RedisUtil.getJedisClient
        //每天   地区   城市   广告
        val key = "date:area:city:ads"

        message.foreach(data=>{
          client.hset(key,data._1,data._2.toString)
        })

        client.close()
      })
    })




    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
