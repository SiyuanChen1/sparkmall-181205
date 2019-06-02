package com.atguigu.sparkmall.realtime

import com.atguigu.sparkmall.common.util.{DateUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  *
  * 一小时广告点击趋势
  */
object Req7AdvClickWindowApplication {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("s")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    val topic ="ads_log"

    val kafkaMessage: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,streamingContext)
    val messageDStream: DStream[KafkaMessage] = kafkaMessage.map(data => {
      val value = data.value()
      val values = value.split(" ")
      KafkaMessage(values(0), values(1), values(2), values(3), values(4))
    })



    val windowDStream: DStream[KafkaMessage] = messageDStream.window(Seconds(60), Seconds(10))


    val timeNodeDStream: DStream[(String, Int)] = windowDStream.map { data => {
      val ts = data.timestamp
      val time: String = DateUtil.parseTimestampToString(ts.toLong)
      val timeNode = time.substring(0, time.size - 1) + "0"
      (timeNode, 1)
    }
    }
    val timeToSumDStream: DStream[(String, Int)] = timeNodeDStream.reduceByKey(_+_)
    val sortDStream = timeToSumDStream.transform(rdd => {
      rdd.sortBy(t => {
        t._1
      }, true)
    })
    sortDStream.print()

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
