package com.atguigu.apitest.sinkTest.hdfs

import java.util.{Random, UUID}

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class HdfsSourceTest extends SourceFunction[String]{
  var running = true
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
//    val hour = Array[String]("00",  "01",  "02",  "03",  "04",  "05",  "06",  "07",  "08",  "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23")
    val hour = Array[String]("00",  "01",  "02",  "03")
    val min = Array[String]("00",  "01",  "02",  "03",  "04",  "05",  "06",  "07",  "08",  "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59")
    val viewType = Array[String]("page_view", "adv_click", "thumbs_up")
    while (true) {
      val date = "2020-02-06 " + hour.toList(new Random().nextInt(hour.size)) + ":" + min.toList(new Random().nextInt(min.size))
      val userId = ((Math.random * 9 + 1) * 100000).toInt
      val viewT = viewType.toList(new Random().nextInt(viewType.size))
      val uuid = UUID.randomUUID()
      val send = date + "," + userId + "," + viewT + "," + uuid
      ctx.collect(send)
      Thread.sleep(200)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
