package com.flink.apitest.util

case class PayEvent(orderId: String,
                    eventType: String,
                    eventTime: String)