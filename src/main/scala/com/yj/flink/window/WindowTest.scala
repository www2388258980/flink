package com.yj.flink.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    val inputStream = env.socketTextStream("localhost", 7777);
    val resultStream = inputStream
      .map(data => {
        val arr = data.split(",");
        (arr(0), arr(2).toDouble);
      })
      .keyBy(_._1)
      //        .window(TumblingEventTimeWindows.of(Time.seconds(15)))  // 滚动时间窗口
      //        .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(3))) // 滚动时间窗口
      //      .window(EventTimeSessionWindows.withGap(Time.seconds(10))) // 会话窗口
      .timeWindow(Time.seconds(10))
      //      .countWindow(10); // 滚动窗口
      .reduce((curr, newstate) => (curr._1, curr._2.min(newstate._2)));

    resultStream.print();

    env.execute("window test");
  }
}

class MyReduceFunction extends ReduceFunction[(String, Double)] {
  override def reduce(t: (String, Double), t1: (String, Double)): (String, Double) = {
    (t._1, t._2.min(t1._2));
  }
}
