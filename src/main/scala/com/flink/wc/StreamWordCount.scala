package com.flink.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.wc
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/18 14:27
  */

// 流处理 word count

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 从外部传入参数
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    // 1. 创建流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(8)
//    env.disableOperatorChaining()

    // 2. 从文本流读取流式数据
    val textDataStream = env.socketTextStream(host, port)

    // 3. 进行转换处理，count
    val dataStream = textDataStream
      .flatMap(_.split("\\s"))
      .filter(_.nonEmpty).startNewChain()
      .map((_, 1))
      .keyBy(0)    // 以二元组中索引下标为0的元素作为key
      .sum(1)

    // 4. 打印输出
    dataStream.print().setParallelism(1)

    // 启动任务
    env.execute()
  }
}
