package com.yj.hia.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * 向kafka写入数据,方便本地测试
  */
class KafkaProducerUtil {
  def main(args: Array[String]): Unit = {
    wirteToKafka("hotitems");
  }

  def wirteToKafka(topic: String): Unit = {
    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.Stringserializer");
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.Stringserializer");

    val producer = new KafkaProducer[String, String](properties);

    // 从文件中读取数据，逐行写入kafka
    val bufferedSource = io.Source.fromFile("G:\\flink\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");
    for (line <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line);
      producer.send(record);
    }

    producer.close();
  }
}
