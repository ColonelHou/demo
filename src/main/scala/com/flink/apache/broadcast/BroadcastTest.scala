package com.flink.apache.broadcast

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1: DataSet[Map[Int, Char]] = env.fromElements((1, '男'), (2, '女')).map(Map(_))
    val ds2 = env.fromElements((102, "风清扬", '1'), (104, "黄蓉", '2'))


    val ds3: DataSet[(Int, Char)] = env.fromElements((1, '男'), (2, '女'))
    val ds4: DataSet[(Int, String, Int)] = env.fromElements((102, "风清扬", 1), (104, "黄蓉", 2))
    // join实例
    ds3.join(ds4).where(0).equalTo(2).map(x => {
      val s1 = x._1
      val s2 = x._2
      (s2._1, s2._2, s1._2)
    }).print()


    ds2.map(new RichMapFunction[(Int, String, Char),(Int, String, Char)] {
      var bc: java.util.List[Map[Int, Char]] = _
      var bcTmp: mutable.Map[Int, Char] = _

      override def open(parameters: Configuration): Unit = {
        import scala.collection.JavaConversions._
        bcTmp = mutable.HashMap()
        bc = getRuntimeContext.getBroadcastVariable("getSex")
        for (x <- bc)bcTmp.putAll(x)
      }

      override def map(value: (Int, String, Char)): (Int, String, Char) = {
        val gender = bcTmp.getOrElse(value._3, '男')
        (value._1, value._2, gender)
      }
    }).withBroadcastSet(ds1, "getSex")
      .print



  }

}
