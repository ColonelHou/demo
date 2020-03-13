package com.flink.apitest.util

// 行为, 渠道(wechat, huawei, xiaomi, )
case class MarketingUserBehavior(userId: String, behavior: String, channel: String, ts: Long)
