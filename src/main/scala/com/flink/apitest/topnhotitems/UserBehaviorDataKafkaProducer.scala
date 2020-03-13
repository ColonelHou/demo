package com.flink.apitest.topnhotitems

import java.util.Properties
import scala.io.Source

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object UserBehaviorDataKafkaProducer {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }

  def writeToKafka(topic: String) : Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val bufferedSource = Source.fromFile("/Users/colonelhou/software/flink-tutorial/Flink0830Tutorial/src/main/resources/UserBehavior.csv")
    for (line <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }
}
