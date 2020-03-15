package com.flink.qf

import org.apache.flink.api.scala._

object WorldCount {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.readTextFile("/Users/houningning/software/demo/pom.xml")
      .flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()
    env.execute("wc")
  }
}
