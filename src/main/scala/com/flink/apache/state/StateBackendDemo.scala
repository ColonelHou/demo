package com.flink.apache.state

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala._

object StateBackendDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    val backend = new RocksDBStateBackend("hdfs://ns1/flink/checkpoints", true)
//    backend.setDbStoragePath("/Users/mac/flink/checkpoint")
//    env.setStateBackend(backend)

    /**
     * 保存到本地或者HDFS中
     */
    //    val checkpointPath = "hdfs://ns1/"
//    val checkpointPath = "file:///Users/houningning/software/flink-1.10.0/data"
//    env.setStateBackend(new FsStateBackend(checkpointPath))

    env.enableCheckpointing(10 * 1000)

    env.socketTextStream("localhost", 9999)
      .flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print("KeyedState测试")

    // 配置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS)))
    env.execute()

  }
}
