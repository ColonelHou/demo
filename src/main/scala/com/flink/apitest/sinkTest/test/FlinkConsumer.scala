package com.flink.apitest.sinkTest.test

import java.util.{Properties, Random}
import org.apache.flink.streaming.api.functions.source.SourceFunction

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.joda.time.DateTime

/**
 * Flink streaming consumer which consumers messages from a kafka topic and populates an elasticsearch index
 */
object FlinkConsumer {

  /**
   * The orchestrator method which initiates the connection to Kafka broker and consumes the data from a kafka topic.
   * The function then initiates a connection with an elasticsearch index and populates the data into the index.
   *
   * @param args command line arguments. If nulls are passed, they default to known values.
   */
  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //      val consumer :FlinkKafkaConsumer[String] =
    //        new FlinkKafkaConsumer[String] (topicName, new CustomerDeserializer(), properties)

    val dataStream: DataStream[EsObject] = env.addSource(new MySource)

    dataStream.print("=========>")
    val elasticSearchConsumer = new ElasticSearchConsumer()
    //
    dataStream.addSink(elasticSearchConsumer.esSinkBuilder.build())
    dataStream.print("---->")

    env.execute("Flink Scala Kafka Invocation")
  }
}

case class EsObject(ltype: String, dtype: String, itemId: String, datetime: String)

class MySource extends SourceFunction[(EsObject)] {
  override def run(ctx: SourceFunction.SourceContext[(EsObject)]): Unit = {
    val listItem = Array("001", "002", "003", "004", "005", "006")
    val list = Array("n", "yn", "yv", "kb")
    val imclc = Array("im", "clc")
    while (true) {
      Thread.sleep(1000)
      val dt = new DateTime()
      val itemId = listItem(new Random().nextInt(listItem.size))
      val dtype = list(new Random().nextInt(list.size))
      val ltype = imclc(new Random().nextInt(imclc.size))
      val datetime = dt.toString("yyyy-MM-dd HH:mm")
      ctx.collect(EsObject(ltype, dtype, itemId, datetime))
    }
  }

  override def cancel(): Unit = {

  }
}