package com.flink.apitest

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/22 10:37
  */
object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // checkpoint设置
    env.enableCheckpointing(10000L)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(true)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // 重启策略配置
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L))
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))

    // 状态后端配置
//    env.setStateBackend(new RocksDBStateBackend(""))

    //    val streamFromFile: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val inputStream = env.socketTextStream("localhost", 7777)

    // Transform操作
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    //
    val processedStream = dataStream
      .keyBy(_.id)
      .flatMap( new TempChangeWarning(10.0) )

    val processedStream2 = dataStream
      .keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double]( {
      case (inputData: SensorReading, None) => ( List.empty, Some(inputData.temperature) )
      case (inputData: SensorReading, lastTemp: Some[Double]) =>
        // 计算两次温度的差值
        val diff = (inputData.temperature - lastTemp.get).abs
        // 判断差值是否大于给定的阈值
        if( diff > 10.0 ){
          ( List((inputData.id, lastTemp.get, inputData.temperature)), Some(inputData.temperature) )
        } else
          ( List.empty, Some(inputData.temperature) )
    } )

    processedStream2.print()

    env.execute("state test")
  }
}

// 自定义 Rich FlatMap Function
class TempChangeWarning(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{
  // 定义一个状态，用于保存上一次的温度值
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 获取到上次的温度值
    val lastTemp = lastTempState.value()
    // 计算两次温度的差值
    val diff = (value.temperature - lastTemp).abs
    // 判断差值是否大于给定的阈值
    if( diff > threshold ){
      out.collect( (value.id, lastTemp, value.temperature) )
    }
    // 更新状态
    lastTempState.update(value.temperature)
  }
}