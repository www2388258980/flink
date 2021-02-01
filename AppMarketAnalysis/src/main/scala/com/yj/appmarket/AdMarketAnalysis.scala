package com.yj.appmarket

import java.sql.Timestamp
import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
  * 广告统计分析
  */
object AdMarketAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    val dataStream = env.readTextFile(getClass.getResource("/AdClickLog.csv").getPath)
      .map(data => {
        val arr = data.split(",");
        AdInputData(arr(0), arr(1), arr(2), arr(3), arr(4).toLong);
      })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(5)) // 周期生成水位线并且延时5秒
          .withTimestampAssigner(new SerializableTimestampAssigner[AdInputData] {
          override def extractTimestamp(element: AdInputData, recordTimestamp: Long): Long = element.timestamp * 1000L
        })
      )
      .keyBy(_.province)
      .timeWindow(Time.days(1), Time.hours(1))
      .aggregate(new AdAggregateFunction, new AdWindowFunction);

    dataStream.print();

    env.execute("ad analysis job");
  }
}

// 定义输入数据源
case class AdInputData(userId: String, adId: String, province: String, city: String, timestamp: Long)

// 定义输出数据源
case class AdOutputData(windowStart: String, windowEnd: String, province: String, cnt: Long)

// 自定义聚合函数AggregateFunction  每来一条数据处理一次
class AdAggregateFunction extends AggregateFunction[AdInputData, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdInputData, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义WindowFunction  处理来自AggregateFunction函数处理过的结果
class AdWindowFunction extends WindowFunction[Long, AdOutputData, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdOutputData]): Unit = {
    val start = new Timestamp(window.getStart).toString;
    val end = new Timestamp(window.getEnd).toString;
    out.collect(AdOutputData(start, end, key, input.head));
  }
}


