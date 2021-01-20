package com.yj.networklfow.impl

import com.yj.networklfow.pojo.PageViewCount
import org.apache.flink.streaming.api.scala.function.{WindowFunction}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class PageViewCountWindowResult extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, window.getStart, window.getEnd, input.iterator.next()));
  }
}
