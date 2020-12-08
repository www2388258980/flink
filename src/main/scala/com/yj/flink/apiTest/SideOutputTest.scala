package com.yj.flink.apiTest

import com.yj.flink.source.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;

    // 读取数据
    val inputStream = env.socketTextStream("localhost", 7777);
    // 先转换成样例类型
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",");
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble);
      });

    val highStream = dataStream
      .process(new SplitTempProcessor(30.0));

    highStream.print("high");

    val lowStream = highStream.getSideOutput(new OutputTag[(String, Long, Double)]("low"));
    lowStream.print("low");

    env.execute("side output test");
  }

}

class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                              out: Collector[SensorReading]): Unit = {
    if (value.temperature > threshold) {
      // 如果温度值大于threhold，输出到主流
      out.collect(value);
    } else {
      // 否则输出到侧输出流
      ctx.output(new OutputTag[(String, Long, Double)]("low"), (value.id, value.timestamp, value.temperature));
    }

  }
}
