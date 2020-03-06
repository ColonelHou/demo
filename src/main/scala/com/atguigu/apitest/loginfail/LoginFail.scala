package com.atguigu.apitest.loginfail

import org.apache.flink.api.common.state.{ListStateDescriptor, ListState}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer



case class LoginEventOld(userId: Long, ip: String, eventType: String, eventTime: Long)

case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/LoginLog.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(line => {
        val lineArray = line.split(",")
        LoginEventOld(lineArray(0).toLong, lineArray(1), lineArray(2), lineArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEventOld](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEventOld): Long = {
          element.eventTime * 1000L
        }
      })
      .keyBy(_.userId)
      .process(new LoginWarning(2))
      .print()

    env.execute("LoginFail Job")

  }
}
class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEventOld, Warning] {
  // 定义状态，保存2秒内的所有登录失败事件
  lazy val loginFailState: ListState[LoginEventOld] = getRuntimeContext.getListState(
    new ListStateDescriptor[LoginEventOld]("login-fail-state", classOf[LoginEventOld])
  )

  override def processElement(value: LoginEventOld, ctx: KeyedProcessFunction[Long, LoginEventOld, Warning]#Context,
                              out: Collector[Warning]): Unit = {
    if (value.eventType == "fail") {
      // 如果是失败，判断之前是否有登录失败事件
      val iter = loginFailState.get().iterator()
      if (iter.hasNext) {
        // 如果已经有登录失败事件，就比较事件时间
        val firstFail = iter.next()
        if (value.eventTime < firstFail.eventTime + 2) {
          // 如果两次间隔小于2秒，输出报警
          out.collect(Warning(value.userId, firstFail.eventTime, value.eventTime, "login fail in 2 seconds."))
        }
        // 更新最近一次的登录失败事件，保存在状态里
        loginFailState.clear()
        loginFailState.add(value)
      } else {
        // 如果是第一次登录失败，直接添加到状态
        loginFailState.add(value)
      }
    } else {
      // 如果是成功，清空状态
      loginFailState.clear()
    }
  }
}
