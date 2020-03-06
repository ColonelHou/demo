package com.atguigu.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/24 9:40
  */
object TableWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val dataStream = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
      )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })
    // 基于env创建 tableEnv
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
    // 从一条流创建一张表，按照字段去定义，并指定事件时间的时间字段
    val dataTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'ts.rowtime)

    // 按照时间开窗聚合统计
    val resultTable: Table = dataTable
      .window( Tumble over 10.seconds on 'ts as 'tw )
      .groupBy('id, 'tw)
      .select('id, 'id.count)

    val resultSqlTable: Table = tableEnv.sqlQuery("select id, count(id) from "
    + dataTable + " group by id, tumble(ts, interval '15' second)")

    val selectedStream: DataStream[(Boolean, (String, Long))] = resultTable
      .toRetractStream[(String, Long)]

    selectedStream.print()

    env.execute("table window test")
  }
}
