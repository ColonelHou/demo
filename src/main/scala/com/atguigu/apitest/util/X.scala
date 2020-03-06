package com.atguigu.apitest.util

import java.util.Properties

import io.rebloom.client.Client
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.{SimpleStringSchema, TypeInformationSerializationSchema}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.util.Collector
import org.joda.time.DateTime



case class Log(id: String, time: Long)
case class Result()
object X {
  def main(args: Array[String]): Unit = {

    println("abc")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 9999)
//    val stream = getKafkaStream()
    val processStream = stream.map(line => {
      Log(line.split(",")(0), line.split(",")(1).toLong)
    }).keyBy(_.id)
      .timeWindow(Time.minutes(1))
      .trigger(new DistinctTrigger)
      .process(new DistinctWindow())


     val integerSerializationSchema =
      new TypeInformationSerializationSchema(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());
    val integerKeyedSerializationSchema =
      new KeyedSerializationSchemaWrapper(integerSerializationSchema);
    /*val pr = new FlinkKafkaProducer(
      "dest",
      integerKeyedSerializationSchema,
      getKafkaConf(),
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE);*/


    env.execute("distinct")

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
      val log = elements.last.id
      if (!client.exists(bloomFilterKey, log)) {
        client.add(bloomFilterKey, log)
      } else {
        out.collect(elements.last)
      }
    }
  }



  def getKafkaStream(): DataStream[String] = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "consumer-group")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")

    env
      .addSource(
        new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), props)
      )
  }
  def getKafkaConf(): Properties = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
//    props.setProperty("group.id", "consumer-group")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")
    props
  }
}
