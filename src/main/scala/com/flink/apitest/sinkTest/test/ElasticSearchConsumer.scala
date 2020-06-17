package com.flink.apitest.sinkTest.test

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.{ElasticsearchSink, RestClientFactory}
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{Requests, RestClientBuilder}

/**
 * ElasticSearch class which acts as a sink for the Kafka topic messages
 */
class ElasticSearchConsumer() {
  val hostname : String = "192.168.0.101"
  val port : Int =  9200
  val scheme : String = "http"

  val httpHosts = new java.util.ArrayList[HttpHost]

  httpHosts.add(new HttpHost(hostname, port, scheme))

  val esSinkBuilder: ElasticsearchSink.Builder[EsObject] =
    new ElasticsearchSink.Builder[EsObject](httpHosts, new ElasticsearchSinkFunction[EsObject]{

      /**
       * This is a customized version of the overridden method in the ElasticsearchSinkFunction
       * which decodes the topic data and loads into the elasticsearch index name
       * @param t -> the data coming from the kafka topic
       * @param runtimeContext -> runtime context
       * @param requestIndexer -> requestIndex of the elasticsearch
       */
      override def process(t: EsObject, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {


        val ltype = t.ltype
        val dtype = t.dtype
        val itemid = t.itemId
        val datetime = t.datetime

        val json = new util.HashMap[String, String]()

        json.put("id", ltype)
        json.put("dtype", dtype)
        json.put("itemid", itemid)
        json.put("datetime", datetime)

        val esIndexName : String = "twitter"

        val requestIndex : IndexRequest = Requests.indexRequest()
          .index(esIndexName)
          .id(ltype)
          .source(json)

        requestIndexer.add(requestIndex)

      }
    }
    )
  // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
  esSinkBuilder.setBulkFlushMaxActions(1)
}
