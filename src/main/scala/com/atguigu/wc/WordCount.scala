package com.atguigu.wc

import org.apache.flink.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.wc
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/18 14:08
  */

// 批处理 word count

object WordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建一个批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 2. 从文件中批量读取数据
    val inputPath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    // 3. 对每一行的单词进行分词平铺展开，根据不同的word进行count聚合
    val wordCountDataSet: AggregateDataSet[(String, Int)] = inputDataSet
      .flatMap(_.split(" "))    // 平铺展开
      .map((_, 1))    // 转换成二元组方便分组聚合
      .groupBy(0)     // 按照二元组中索引为0的元素进行分组
      .sum(1)     // 按照二元组中索引为1的元素进行聚合

    // 4. 打印输出
    wordCountDataSet.print()
  }
}
