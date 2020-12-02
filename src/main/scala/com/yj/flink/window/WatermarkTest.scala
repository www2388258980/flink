package com.yj.flink.window

import com.yj.flink.source.SensorReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WatermarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;

    // 读取数据
    val inputStream = env.socketTextStream("localhost", 7777);
    // 先转换成样例类型
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",");
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble);
      })
      //      .assignAscendingTimestamps(_.timestamp * 1000L) // 升序数据提取时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(500)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L
    })
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .reduce((t1, t2) => SensorReading(t1.id, t2.timestamp, t1.temperature.min(t2.temperature)));

    dataStream.print();

    env.execute("watermark test");

  }
}
