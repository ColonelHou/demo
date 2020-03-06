package com.atguigu.apitest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/24 9:26
  */
object TableTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val dataStream = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
      )
    // 基于env创建 tableEnv
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
    // 从一条流创建一张表
    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    // 从表里选取特定的数据
    val selectedTable: Table = dataTable.select('id, 'temperature)
      .filter("id = 'sensor_1'")

    val selectedStream: DataStream[(String, Double)] = selectedTable
      .toAppendStream[(String, Double)]

    selectedStream.print()

    env.execute("table test")

  }
}
