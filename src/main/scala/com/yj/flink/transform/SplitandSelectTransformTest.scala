package com.yj.flink.transform

import com.yj.flink.source.SensorReading
import org.apache.flink.streaming.api.scala._

object SplitandSelectTransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    val inputStream = env.readTextFile("G:\\flink\\src\\main\\resources\\hello.txt");
    // 分流,将温度分成高温流和低温流
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",");
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble);
      })
      .keyBy("id")
      .split(data => {
        if (data.temperature > 30) {
          Seq("high");
        } else {
          Seq("low");
        }
      });
    val lowSteam = dataStream.select("low");
    val highStream = dataStream.select("high");
    val allStream = dataStream.select();
    lowSteam.print("low");
    highStream.print("high");
    allStream.print("all");

    env.execute("split and select test");
  }
}
