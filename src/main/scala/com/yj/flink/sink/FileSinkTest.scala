package com.yj.flink.sink

import com.yj.flink.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._


object FileSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    val inputStream = env.readTextFile("G:\\flink\\src\\main\\resources\\hello.txt");
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",");
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble);
      });
    // 输出到文件
    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("G:\\flink\\src\\main\\resources\\hello2.txt"),
        new SimpleStringEncoder[SensorReading]()
      ).build()
    );

    env.execute("file sink test");
  }
}
