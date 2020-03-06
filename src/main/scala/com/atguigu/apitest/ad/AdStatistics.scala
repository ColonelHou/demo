package com.atguigu.apitest.ad

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timestamp: Long)
case class CountByProvice(windowEnd: String, province: String, count: Long)
// 黑名单报警信息
case class BlackListWarning(userId: Long, adId: Long, msg: String)
object AdStatistics {
  // process部分主输出用于下边的统计，黑名单部分用侧输出流获取
  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val addEventStream = env.readTextFile("/Users/houningning/software/demo/src/main/resources/AdClickLog.csv")
      .map(data => {
        val dataArray = data.split(",")
        AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong,
          dataArray(2).trim, dataArray(3).trim, dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)

    // 自定义process function过滤刷单行为，过滤掉的数据加入侧输出流
    val filterBlackListStream = addEventStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(30)) // 超过10次点击量就报警; 刷广告

    val adCountStream = filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.minutes(10))
      .aggregate(new AdCountAgg(), new AdCountResult())

    adCountStream.print()
    // 哪个算子输出的侧流就用哪个getSideOutput
    filterBlackListStream.getSideOutput(blackListOutputTag).print("blackList")

    env.execute("AdStatistics Job")
  }

  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {

    // 定义状态，保存当前用户对当前广告的点击量
    lazy val countState: ValueState[Long] = getRuntimeContext
      .getState(new ValueStateDescriptor[Long]("countState", classOf[Long]))
    // 保存是否发送过黑名单的状态
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext
      .getState(new ValueStateDescriptor[Boolean]("isSent-state", classOf[Boolean]))
    // 保存定时器触发的时间戳
    lazy val resetTimer: ValueState[Long] = getRuntimeContext
      .getState(new ValueStateDescriptor[Long]("resetTime-state", classOf[Long]))

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long),
      AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      // 取出count状态
      val curCount = countState.value()

      // 如果是第一次处理，注册定时器，每天00：00触发
      if (curCount == 0) {
        val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
        resetTimer.update(ts)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      // 判断计数是否达到上限，如果到达则加入黑名单
      if (curCount >= maxCount) {
        // 判断是否发送过黑名单，只发送一次
        if (!isSentBlackList.value()) {
          isSentBlackList.update(true)
          // 输出到侧输出流
          ctx.output(blackListOutputTag, BlackListWarning(value.userId, value.adId,
            "Click over " + maxCount + " times today."))
        }
        return // 如果判断已经超过限制数，则不应输出数据到主流
      }
      // 计数状态加1，输出数据到主流
      countState.update(curCount + 1)
      out.collect(value)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent,
      AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      // 定时器触发时，清空状态
      if (timestamp == resetTimer.value()) {
        isSentBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }
    }
  }

  class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def merge(acc: Long, acc1: Long): Long = acc + acc1

    override def getResult(acc: Long): Long = acc

    override def add(in: AdClickEvent, acc: Long): Long = acc + 1
  }

  class AdCountResult() extends WindowFunction[Long, CountByProvice, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long],
                       out: Collector[CountByProvice]): Unit = {
      out.collect(CountByProvice(new Timestamp(window.getEnd).toString, key, input.iterator.next))
    }
  }

}
