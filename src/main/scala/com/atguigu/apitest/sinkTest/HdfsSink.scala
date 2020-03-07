package com.atguigu.apitest.sinkTest

import com.atguigu.apitest.kafka.KafkaUtils
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object HdfsSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaSource = new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), KafkaUtils.getKafkaConf())
    val stream = env.addSource(kafkaSource).map(x => x)

    // 设置一个滚动策略
    val policy: DefaultRollingPolicy[String, String] = DefaultRollingPolicy.create()
      .withMaxPartSize(1024 * 1) // 1KB
      .withInactivityInterval(60 * 1000) //  60s空闲，就滚动写入新的文件
      .withRolloverInterval(2000) // 每隔2秒生成一个文件
      .build() // 创建
    // 创建hdfs Sink
    val hdfsSink: StreamingFileSink[String] = StreamingFileSink.forRowFormat[String](
      new Path("/bitauto/"),
      new SimpleStringEncoder("UTF-8"))
      .withRollingPolicy(policy)
      .withBucketCheckInterval(1000) // 检查间隔时间
      .build()

    stream.addSink(hdfsSink)



  }

}
