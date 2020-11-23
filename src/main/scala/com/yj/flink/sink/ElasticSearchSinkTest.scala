package com.yj.flink.sink

import java.util

import com.yj.flink.source.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object ElasticSearchSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    val inputStream = env.readTextFile("G:\\flink\\src\\main\\resources\\hello.txt");
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",");
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble);
      });

    // 定义HttpHosts
    val httpHosts = new util.ArrayList[HttpHost]();
    httpHosts.add(new HttpHost("localhost", 9200));
    // 自定义写入es的ESSinkFunction
    val myEsSinkFun = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        // 创建一个map作为dataSource
        val dataSource = new util.HashMap[String, String]();
        dataSource.put("id", t.id);
        dataSource.put("temperature", t.temperature.toString);
        dataSource.put("ts", t.timestamp.toString);

        // 创建index request,用于发送http请求
        val indexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("readdingdata")
          .source(dataSource);
      }
    };

    dataStream.addSink(new ElasticsearchSink.Builder[SensorReading](httpHosts, myEsSinkFun)
      .build()
    );

    env.execute("elasticSearch sink test");
  }
}
