package com.atguigu.apitest.sinkTest.hdfs

import java.io.{OutputStream, PrintStream}
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.serialization.{Encoder, SimpleStringEncoder}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.core.fs.Path
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.{DefaultRollingPolicy, OnCheckpointRollingPolicy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.{DateTimeBucketAssigner, SimpleVersionedStringSerializer}
import org.apache.flink.streaming.api.windowing.time.Time

case class KafkaLog(uid: String, itemId: String, clcType: String, uuid: String)
object HdfsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.enableCheckpointing(20000)
//    env.setStateBackend(new FsStateBackend("/Users/houningning/bak/flink/checkpoints").asInstanceOf[StateBackend])
    env.setParallelism(1)
    val stream = env.addSource(new HdfsSourceTest()).map(x => x)
//    val streamObj = stream.map(x => {
//      val arr = x.split(",")
//      KafkaLog(arr(0), arr(1), arr(2), arr(3))
//    }).keyBy(_.uid).timeWindow(Time.seconds(5))

    val po = DefaultRollingPolicy.builder
      .withRolloverInterval(TimeUnit.MINUTES.toMillis(2))
      .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
      .withMaxPartSize(1024 * 1024 * 1024)
      .build

    StreamSink.getHdfsSinkExe(stream , "/Users/houningning/bak/flink", new EventTimeBucketAssigner, po, "yiche", ".log")
//    val sink = StreamSink.test("/Users/houningning/bak/flink")
    stream.print()
    env.execute("self define source function")

  }

  @SerialVersionUID(987325769970523326L)
  final class KeyBucketAssigner extends BucketAssigner[Tuple2[Integer, Integer], String] {
    override def getBucketId(element: Tuple2[Integer, Integer], context: BucketAssigner.Context): String = String.valueOf(element.f0)

    override def getSerializer: SimpleVersionedSerializer[String] = SimpleVersionedStringSerializer.INSTANCE
  }

}
