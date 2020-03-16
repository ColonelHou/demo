package com.flink.apache.broadcast

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object BroadcastStreamVar {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1: DataStream[(Int, Char)] = env.fromElements((1, '男'), (2, '女'))
    val ds2 = env.socketTextStream("localhost", 9999)
      .map(line => {
        val arr = line.split(",")
        (arr(0).toInt, arr(1))
      })
    val desc = new MapStateDescriptor("genderInfo",
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.CHAR_TYPE_INFO)
    // 把ds1广播流变量, desc:描述信息
    val bcStream = ds1.broadcast(desc)
    ds2.connect(bcStream)
      .process(new BroadcastProcessFunction[(Int, String), (Int, Char), (Int, String, Char)] {
        // 会执行多次, 每次处理非广播流中的元素
      override def processElement(value: (Int, String), ctx: BroadcastProcessFunction[(Int, String), (Int, Char), (Int, String, Char)]#ReadOnlyContext, out: Collector[(Int, String, Char)]): Unit = {
        val gender = ctx.getBroadcastState(desc).get(value._1)
        println("结合广播流-----------------------> " + value._1 + "," + value._2)
        out.collect((value._1, value._2, gender))
      }

        /**
         * 执行多次, 每次执行广播流中的元素
         * @param value
         * @param ctx
         * @param out
         */
      override def processBroadcastElement(value: (Int, Char), ctx: BroadcastProcessFunction[(Int, String), (Int, Char), (Int, String, Char)]#Context, out: Collector[(Int, String, Char)]): Unit = {
        println("广播流变量处理 => " + value._1 + "," + value._2)
        ctx.getBroadcastState(desc).put(value._1, value._2)
      }
    }).print()
    env.execute("broadcast stream")

  }
}
