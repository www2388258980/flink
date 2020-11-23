package com.yj.flink.sink

import com.yj.flink.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    val inputStream = env.readTextFile("G:\\flink\\src\\main\\resources\\hello.txt");
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",");
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString;
      });
    // 输出到kafka
    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092",
      "sinkTest", new SimpleStringSchema()));
    env.execute("kafka sink test");
  }
}
