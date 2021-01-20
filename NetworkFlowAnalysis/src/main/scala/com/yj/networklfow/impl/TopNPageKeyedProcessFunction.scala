package com.yj.networklfow.impl

import java.sql.Timestamp

import com.yj.networklfow.pojo.PageViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class TopNPageKeyedProcessFunction(top: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {

  //  lazy val pageViewCountListState: ListState[PageViewCount] = getRuntimeContext.getListState(
  //    new ListStateDescriptor[PageViewCount]("pageViewCountListState", classOf[PageViewCount]));
  lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Long]("pageViewCountMapState", classOf[String], classOf[Long])
  );

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    //    pageViewCountListState.add(value);
    pageViewCountMapState.put(value.url, value.cnt);
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
    // 另外定义一个定时器，1分钟之后触发，这时候窗口已经完全关闭
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60000L);
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    //    val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer();
    //    val iter = pageViewCountListState.get().iterator();
    //    while (iter.hasNext) {
    //      allPageViewCounts += iter.next();
    //    }

    // 判断定时器触发时间，如果是窗口已经彻底关闭时间，则清空状态
    if (timestamp == ctx.getCurrentKey + 60000L) {
      pageViewCountMapState.clear();
      return;
    }

    val allPageViewCounts: ListBuffer[(String, Long)] = ListBuffer();
    val iter = pageViewCountMapState.entries().iterator();
    while (iter.hasNext) {
      val entry = iter.next();
      allPageViewCounts += ((entry.getKey, entry.getValue));
    }
    // 将排名信息格式化成String，便于打印输出可视化展示
    val result: StringBuilder = new StringBuilder("\n============================================\n\n");
    result
      .append("窗口结束时间：")
      .append(new Timestamp(timestamp - 1))
      .append("\n");

    val sortedPageViewCount = allPageViewCounts.sortWith(_._2 > _._2).take(top);

    // 遍历结果列表中的每个ItemViewCount，输出到一行
    for (i <- sortedPageViewCount.indices) {
      val currentItemViewCount = sortedPageViewCount(i);
      var lenBlank: StringBuilder = new StringBuilder;
      for (j <- 0 to (10 - currentItemViewCount._1.toString.length)) {
        lenBlank.append(" ");
      }
      result.append("NO").append(i + 1).append(": \t")
        .append("页面URL = ").append(currentItemViewCount._1).append(lenBlank + "\t")
        .append("热门度 = ").append(currentItemViewCount._2).append("\n")
    }


    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
