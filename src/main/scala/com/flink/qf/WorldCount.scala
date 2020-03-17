package com.flink.qf

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object WorldCount {

  def main(args: Array[String]): Unit = {
  /*  if (null = args || args.length < 4) {
      println(
        """
          |请输入参数, --input <源path> --output <目的path>
          |""".stripMargin)
    }
    val tool = ParameterTool.fromArgs(args)
    val inputPath = tool.get("input")*/


    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.readTextFile("/Users/houningning/software/demo/pom.xml")
      .flatMap(_.split("\\s+")) // Task1: 以上是1
      .filter(_.nonEmpty).setParallelism(2) // Task2: 并行是2
      .map((_, 1)).setParallelism(3)
      .groupBy(0) // Task3: 并行度是3
      .sum(1).setParallelism(2) // Task4: 并行度是2
        .print
//    env.execute("wc")
  }
}
