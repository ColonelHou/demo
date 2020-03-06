package com.atguigu.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/19 10:55
  */

// 创建一个传感器数据样例类
case class SensorReading( id: String, timestamp: Long, temperature: Double )

object SourceTest {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val  env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.disableOperatorChaining()

    // 1. 从集合中读取数据
    val stream1 = env.fromCollection(
      List(
        SensorReading("sensor_1", 1547718199, 35.8),
        SensorReading("sensor_6", 1547718201, 15.4),
        SensorReading("sensor_7", 1547718202, 6.72),
        SensorReading("sensor_10", 1547718205, 38.1)
      )
    )

//    env.fromElements(0, 1.2, "hello", true)

    // 2. 从文件中读取数据
    val stream2 = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // 3. 真正的流处理，从kafka中读取数据
//    env.socketTextStream("localhost", 7777)
    // 先创建kafka相关配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream3 = env.addSource( new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties) )

    // 4. 自定义输入源
    val stream4 = env.addSource( new SensorSource() )

    // 打印输出
    stream4.print().setParallelism(1)

    env.execute("source test")
  }
}

// 自定义的source function类
class SensorSource() extends SourceFunction[SensorReading]{
  // 先定义一个标识位，用于控制是否发送数据
  var running: Boolean = true

  override def cancel(): Unit = {
    running = false
  }

  // 核心run方法，如果running为true，不停地发出数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 定义一个随机数发生器
    val rand = new Random()
    // 随机生成10个传感器的初始温度值，接下来在它的基础上随机变动
    var curTemp = 1.to(10)
      .map(
        // 1到10的序列，转换成二元组（id，温度值）
        i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
      )

    // 如果没有被cancel，无限循环产生数据
    while(running){
      // 在上次的温度值基础上，微小改变，生成当前新的温度值
      curTemp = curTemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      // 获取当前的系统时间戳
      val curTime = System.currentTimeMillis()
      // 把10个数据都添加上时间戳，全部输出
      curTemp.foreach(
        data => ctx.collect(SensorReading(data._1, curTime, data._2))
      )
      // 间隔500ms
      Thread.sleep(500)
    }
  }
}