package com.atguigu.apitest.sinkTest

import java.util.Properties

import io.rebloom.client.Client
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.Collector
import org.joda.time.DateTime

case class Log(id: String, time: String)
case class Result()

object Streaming {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), getKafkaConf()))
    val dataStream: DataStream[String] = inputStream
      .map(line => {
        Log(line.split(",")(0), line.split(",")(1))
      }).keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .trigger(new DistinctTrigger)
      .process(new DistinctWindow()).map(x => x.id + "|" + x.time)
    dataStream.print("Driver print")

    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092", "dest", new SimpleStringSchema()))
    env.execute("kafka distinct")
  }

  class DistinctTrigger extends Trigger[Log, TimeWindow] {
    override def onElement(element: Log, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
  }
  class DistinctWindow extends ProcessWindowFunction[Log, Log, String, TimeWindow] {
    //    lazy val jedis = new Jedis("localhost", 6379)
    lazy val client = new Client("localhost", 6379)
    override def process(key: String, context: Context, elements: Iterable[Log], out: Collector[Log]): Unit = {
      val bloomFilterKey = "DISTINCT_LOG_KEY_" + new DateTime().toString("yyyy-MM-dd")
      println("---->bloomFilter Key > " + new DateTime().toString())
      val log = elements.last
      val uid = log.id
      println("===========单条日志处理 id ======> " + uid.toString + "\t 存在: " + client.exists(bloomFilterKey, uid))
      if (client.exists(bloomFilterKey, uid)) {
        println("----->redis 已存在 " + log.toString)
      } else {
        println("-----> 发送 " + log.toString)
        client.add(bloomFilterKey, uid)
        out.collect(log)
      }
    }
  }

  def getKafkaConf(): Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    properties
  }
}
