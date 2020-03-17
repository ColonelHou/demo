package com.flink.apache.streaming

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

object StreamSql {

  def main(args: Array[String]): Unit = {
    val execEnv = ExecutionEnvironment.getExecutionEnvironment
    val tblEnv: BatchTableEnvironment = BatchTableEnvironment.create(execEnv)
    val path = "/Users/houningning/software/demo/src/main/scala/com/flink/apache/streaming/tb1.log"
  }
}
