package com.yj.hia.impl


import com.yj.hia.pojo.ItemViewCount
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 自定义窗口函数WindowFunction
class ItemViewWindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {

  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val cnt = input.iterator.next;
    val windowEnd = window.getEnd;
    out.collect(ItemViewCount(key, windowEnd, cnt))
  }
}
