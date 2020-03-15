package com.flink.apache.kafka

import java.lang
import java.nio.charset.StandardCharsets
import java.util.Properties

import com.flink.apitest.kafka.KafkaUtils
import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction, RichFlatMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.internal.KafkaConsumerThread
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object ConsumerProducer {

  val outputTag = new OutputTag[(String, Int)]("体温异常")
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // kafka checkpoint开启
    env.enableCheckpointing(5000)
    // https://www.jianshu.com/p/efa010893a410
    // https://www.jianshu.com/p/3cd2ab1dd311
    env.setStateBackend(new FsStateBackend("hdfs://ns1/checkpoint"))

    val prop = KafkaUtils.getKafkaConf()
    val stream = env.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), prop))


    // 分流1
    val tuple = stream.map(x => {
      (x.split("\\s+")(0), x.split("\\s+")(1).toDouble)
    }).filter(new FilterFunction[(String, Double)] {
      override def filter(value: (String, Double)): Boolean = {
        value._2 != 0
      }
    }).filter(new RichFilterFunction[(String, Double)] {
      override def open(parameters: Configuration): Unit = super.open(parameters)
      override def filter(value: (String, Double)): Boolean = {
//        getRuntimeContext
        value._2 != 0
      }
    }).flatMap(new RichFlatMapFunction[(String, Double), (String, Double)] {
      var producer: KafkaProducer[String, String] = null
      override def open(parameters: Configuration): Unit = {
        producer = new KafkaProducer[String, String](KafkaUtils.getKafkaConf())
      }
      override def close(): Unit = {
        if (null != producer) {
          producer.close()
        }
      }
      override def flatMap(value: (String, Double), out: Collector[(String, Double)]): Unit = {
        if (value._2 > 37) {
          producer.send(new ProducerRecord[String, String]("alarmTopic", null, value._1))
        } else {
          out.collect(value)
        }
      }
    })
    val s1 = tuple.split(tuple => (if(tuple._2 > 36 && tuple._2< 37) Seq("normal") else Seq("exception")))
    val yes = s1.select("normal")
    val no = s1.select("exception")
    // 合并流 union(可多个流,类型必须一样,) connect(只能两个流,可不一样)
    val conntectedStream = yes.connect(no).map(yes => {yes._2}, no => {no._2})
    val u = yes.union(no)

    // 分流2
    val sideStream = tuple.process(new SplitStream)
    sideStream.getSideOutput(outputTag)

    stream.addSink(new FlinkKafkaProducer[String]("dest", new KafkaSerializationSchema[String] {
      override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        new ProducerRecord[Array[Byte], Array[Byte]]("dest", element.getBytes(StandardCharsets.UTF_8))
      }
    }, prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))


    env.execute("read and write kafka")
  }

  class SplitStream extends ProcessFunction[(String, Double), (String, Double)] {
    override def processElement(value: (String, Double), ctx: ProcessFunction[(String, Double), (String, Double)]#Context, out: Collector[(String, Double)]): Unit = {
      if (value._2 > 36 && value._2 < 37 ) {
        out.collect(value)
      } else {
        ctx.output(outputTag, value)
      }
    }
  }

}
