package com.atguigu.apitest.sinkTest.hdfs

import com.atguigu.apitest.sinkTest.Streaming.DistinctTrigger
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.{DefaultRollingPolicy, OnCheckpointRollingPolicy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend

case class KafkaLog(uid: String, itemId: String, clcType: String, uuid: String)
object HdfsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(20000)
    env.setStateBackend(new FsStateBackend("/Users/houningning/bak/flink/checkpoints").asInstanceOf[StateBackend])
    env.setParallelism(1)
    val stream = env.addSource(new HdfsSourceTest()).map(x => x)
    /*val streamObj = stream.map(x => {
      val arr = x.split(",")
      KafkaLog(arr(0), arr(1), arr(2), arr(3))
    }).keyBy(_.uid).timeWindow(Time.seconds(5))*/


    val policy: DefaultRollingPolicy[String, String] = DefaultRollingPolicy.create()
      .withMaxPartSize(1024 * 100)
        .withInactivityInterval(2000)
        .withRolloverInterval(10000)
        .build()
    val sink = StreamingFileSink.forRowFormat[String](
      new Path("/Users/houningning/bak/flink"),
      new SimpleStringEncoder[String]("UTF-8")
    ).withRollingPolicy(policy)
      .withBucketAssigner(new EventTimeBucketAssigner)
//        .withBucketCheckInterval(1000)
//        .build()
//    stream.print()
//    stream.addSink(sink).name("Sink HDFS")

    env.execute("self define source function")

  }

}
