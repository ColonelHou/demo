package com.atguigu.apitest.sinkTest

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.sinkTest
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/19 16:33
  */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    /*val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    // Transform操作
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    // 定义一个Flink jedis config
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    dataStream.addSink( new RedisSink[SensorReading](conf, new MyRedisMapper) )

    env.execute("redis sink test")*/

  }
}
/*
// 自定义一个RedisMapper类
class MyRedisMapper extends RedisMapper[SensorReading]{
  override def getCommandDescription: RedisCommandDescription = {
    // 保存到redis的命令，存成哈希表 HSET sensor_temp key value
    new RedisCommandDescription( RedisCommand.HSET, "sensor_temp" )
  }

  override def getValueFromData(t: SensorReading): String = t.temperature.toString

  override def getKeyFromData(t: SensorReading): String = t.id
}*/
