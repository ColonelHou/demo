package com.flink.apitest.sinkTest.hdfs

import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

class EventTimeBucketAssigner extends BucketAssigner [String, String] {
  override def getBucketId(element: String, context: BucketAssigner.Context): String = {
    val dateTime = DateTime.parse(element.split(",")(0), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm"))
    val year = dateTime.getYear
    val month = "%02d".format(dateTime.getMonthOfYear)
    val day = "%02d".format(dateTime.getDayOfMonth)
    val hour = "%02d".format(dateTime.getHourOfDay)
    val min = "%02d".format(dateTime.getMinuteOfHour)
    val dir = year + "-" + month + "-" + day + "/" + hour
    dir
  }

  override def getSerializer: SimpleVersionedSerializer[String] = {
    SimpleVersionedStringSerializer.INSTANCE
  }
}
