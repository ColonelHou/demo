package com.flink.apitest.util

case class ItemViewCount(itemId: Long,
                         windowEnd: Long,
                         count: Long)
