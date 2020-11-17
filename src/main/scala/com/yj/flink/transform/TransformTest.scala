package com.yj.flink.transform

import com.yj.flink.source.SensorReading
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    // 读取数据
    val inputStream = env.readTextFile("G:\\flink\\src\\main\\resources\\hello.txt");
    // 先转换成样例类型
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",");
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble);
      })
      .keyBy("id")
      .minBy("temperature");
    //    dataStream.print();

    val dataStream2 = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .filter(data => data.id.equals("sensor_1"))
      .keyBy("id")
      .reduce((oldState, newState) => {
        SensorReading(oldState.id, newState.timestamp, oldState.temperature.min(newState.temperature))
      });
    dataStream2.print();
    // 执行
    env.execute("transform test");
  }
}
