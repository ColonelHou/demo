package com.flink.apitest.sinkTest

import java.lang

import com.flink.apitest.kafka.KafkaUtils
import com.flink.apitest.sinkTest.hdfs.HdfsSourceTest
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}

/**
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --property print.key=true
 * 2020-02-06 02:01	354675
 */
object NewKafkaSink {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new HdfsSourceTest)
    val kvStream = stream.map(x => (x.split(",")(0), x.split(",")(1)))

    //第一种
//    stream.addSink(new FlinkKafkaProducer[String]("localhost:9092", "test", new SimpleStringSchema()))
    // 第二种: 写入kv
    kvStream.print()
    kvStream.addSink(new FlinkKafkaProducer[(String, String)](
      "test",
      new KafkaSerializationSchema[(String, String)] {
        override def serialize(t: (String, String), aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
          new ProducerRecord("test", t._1.getBytes, t._2.getBytes)
        }
      },
      KafkaUtils.getKafkaConf(),
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE
    ))
    env.execute("New Kafka Producer Write")

  }
}
