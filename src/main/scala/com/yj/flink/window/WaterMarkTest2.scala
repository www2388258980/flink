package com.yj.flink.window

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

/*
 * 测试数据
 * flink1,1593421135000
 * flink2,1593421135000
 * flink3,1593421135000
 * flink4,1593421135000
 * flink1,1593421147000
 * flink2,1593421147000
 * flink3,1593421147000
 * flink4,1593421147000
 */

object WaterMarkTest2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    // 使用EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // 设置并行度为 1,默认并行度是当前机器的 cpu 数量
    //    env.setParallelism(1);

    val text = env.socketTextStream("127.0.0.1", 9999, '\n');
    //解析输入的数据
    val inputMap = text.map(line => {
      val arr = line.split(",")
      (arr(0), arr(1).toLong)
    });
    // 抽取 timestamp 和生成 watermark
    val waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
      var currentMaxTimestamp = 0L
      var maxOutOfOrderness = 10000L // 最大允许的乱序时间是10s

      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      // 定义生成 watermark 的逻辑, 默认 100ms 被调用一次
      // 当前最大的时间点 - 允许的最大时间
      override def getCurrentWatermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)


      // 提取 timestamp
      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._2
        // 这里想象一个迟到的数据时间，所以这里得到的是当前数据进入的最大时间点
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        val id = Thread.currentThread().getId
        println("currentThreadId:" + id + ",key:" + element._1 + ",eventtime:[" + element._2 + "|" +
          sdf.format(element._2) + "],currentMaxTimestamp:[" + currentMaxTimestamp + "|" +
          sdf.format(currentMaxTimestamp) + "],watermark:[" + getCurrentWatermark().getTimestamp + "|" +
          sdf.format(getCurrentWatermark().getTimestamp) + "]")
        timestamp
      }
    });

    // 保存被丢弃的数据, 定义一个 outputTag 来标识
    //    val outputTag = new OutputTag[Tuple2[String, Long]]("late-data") {}

    // 分组, 聚合
    val window = waterMarkStream.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(3))) //按照消息的EventTime分配窗口，和调用TimeWindow效果一样
      //      .allowedLateness(Time.seconds(2))   //在 WaterMark 基础上还可以延迟2s, 即：数据迟到 2s
      //      .sideOutputLateData(outputTag)
      .apply(new WindowFunction[(String, Long), String, Tuple, TimeWindow] {

      /**
        * Evaluates the window and outputs none or several elements.
        *
        * @param key    The key for which this window is evaluated.
        * @param window The window that is being evaluated.
        * @param input  The elements in the window being evaluated.
        * @param out    A collector for emitting elements.
        * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
        *
        *                   对 window 内的数据进行排序，保证数据的顺序
        */
      override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
        println("key值：", key)
        val keyStr = key.toString
        val arrBuf = ArrayBuffer[Long]()
        val ite = input.iterator
        while (ite.hasNext) {
          val tup2 = ite.next()
          arrBuf.append(tup2._2)
        }

        val arr = arrBuf.toArray
        Sorting.quickSort(arr)

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        val result = keyStr + "," + arr.length + ", data_range[" +
          sdf.format(arr.head) + "," + sdf.format(arr.last) + "], window [" + // 数据的开始时间 和 结束时间
          sdf.format(window.getStart) + "," + sdf.format(window.getEnd) + ")" // 该窗口的开始时间 和 结束时间[) -> 记得左闭右开
        out.collect(result)
      }
    });

    // 侧输出流
    //    val sideOutput: DataStream[Tuple2[String, Long]] = window.getSideOutput(outputTag)
    //    sideOutput.print("side_lated_data")

    window.print();

    env.execute("watermark test");

  }
}
