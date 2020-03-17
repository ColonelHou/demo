package com.flink.apache.distributeCache

import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer
import scala.io.Source

object DistributeCacheFile {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.registerCachedFile("hdfs://ns1/gender.txt", "genderinfo")
    env.socketTextStream("localhost", 9999)
      .filter(_.trim.nonEmpty)
      .map(new RichMapFunction[String, (Int, String, Char, String)] {

        var list: ListBuffer[(Int, String)] = new ListBuffer()
        var map: Map[Int, String] = _
        override def open(parameters: Configuration): Unit = {
          val file: File = getRuntimeContext.getDistributedCache.getFile("genderinfo")
          val src = Source.fromFile(file)
          val lines = src.getLines().toList
          for (ll <- lines) {
            val arr = ll.split(",")
            val id = arr(0).trim.toInt
            val gender = arr(1).trim
            list.append((id, gender))
          }
          map = list.toMap
          src.close()
        }

        override def map(value: String): (Int, String, Char, String) = {
          val arr = value.split(",")
          (arr(0).toInt, arr(1), 'f', map.getOrElse(arr(2).toInt, "男"))
        }
      })
    env.execute("distribute cache file")
  }
}
