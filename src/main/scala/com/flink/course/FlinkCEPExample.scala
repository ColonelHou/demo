package com.flink.course

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

object FlinkCEPExample {

  case class LoginEvent(id: String, username: String, eventType: String, eventTime: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    /**
     * 结果：
     * 1. 用户名:张三，登录时间:1577080457,1577080458,1577080460
     *    用户名:张三, 登录时间:1577080458,1577080460,1577080462
     */
    val stream = env
      .fromElements(
        LoginEvent("1", "张三", "fail", "1577080457"),
        LoginEvent("2", "张三", "fail", "1577080458"),
        LoginEvent("3", "张三", "fail", "1577080460"),
        LoginEvent("4", "李四", "fail", "1577080458"),
        LoginEvent("5", "李四", "success", "1577080462"),
        LoginEvent("6", "张三", "fail", "1577080462")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy(_.id)

    val pattern = Pattern
      .begin[LoginEvent]("first").where(_.eventType == "fail")
      .next("second").where(_.eventType == "fail")
      .next("third").where(_.eventType == "fail")
      .within(Time.seconds(10))

    val patternStream = CEP.pattern(stream, pattern)

    val loginFailStream = patternStream
      .select((pattern: Map[String, Iterable[LoginEvent]]) => {
        val first = pattern.getOrElse("first", null).iterator.next()
        val second = pattern.getOrElse("second", null).iterator.next()
        val third = pattern.getOrElse("third", null).iterator.next()

        "userId是： " + first.username + " 的用户， 10s 之内连续登录失败了三次，" + "ip地址分别是： " + first.eventTime + "; " + second.eventTime + "; " + third.eventTime
      })

    loginFailStream.print()

    env.execute()
  }
}
