package com.atguigu.apitest.sinkTest

import java.util

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.sinkTest
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/19 16:48
  */
object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    // Transform操作
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          // 包装好要发送的数据
          val dataSource = new util.HashMap[String, String]()
          dataSource.put("sensor_id", element.id)
          dataSource.put("temperature", element.temperature.toString)
          dataSource.put("ts", element.timestamp.toString)
          // 创建一个index request
          val indexReq = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata")
            .source(dataSource)
          // 用indexer发送请求
          indexer.add(indexReq)
        }
      }
    )
    esSinkBuilder.setBulkFlushMaxActions(1)
    dataStream.addSink( esSinkBuilder.build() )

    env.execute("es sink test")
  }
}
