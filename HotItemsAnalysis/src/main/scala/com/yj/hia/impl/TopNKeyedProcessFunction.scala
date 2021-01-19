package com.yj.hia.impl

import java.sql.Timestamp

import com.yj.hia.pojo.ItemViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class TopNKeyedProcessFunction(top: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  // 定义keyedstate
  var itemViewCountListState: ListState[ItemViewCount] = _;

  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("listKeyedState",
      classOf[ItemViewCount]));
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long,
    ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每来一条数据，直接加入ListState
    itemViewCountListState.add(value)
    // 注册一个windowEnd + 1之后触发的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    // 为了方便排序，另外定义一个ListBuffer，保存ListState里面的所有数据
    val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
    val iter = itemViewCountListState.get().iterator()
    while (iter.hasNext) {
      allItemViewCounts += iter.next()
    }

    // 清空状态
    itemViewCountListState.clear()

    // 按照count大小排序，取前n个
    val sortedItemViewCounts = allItemViewCounts.sortBy(_.cnt)(Ordering.Long.reverse).take(top)

    // 将排名信息格式化成String，便于打印输出可视化展示
    val result: StringBuilder = new StringBuilder("\n============================================\n\n");
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");


    // 遍历结果列表中的每个ItemViewCount，输出到一行
    for (i <- sortedItemViewCounts.indices) {
      val currentItemViewCount = sortedItemViewCounts(i);
      var lenBlank: StringBuilder = new StringBuilder;
      for (j <- 0 to (10 - currentItemViewCount.itemId.toString.length)) {
        lenBlank.append(" ");
      }
      result.append("NO").append(i + 1).append(": \t")
        .append("商品ID = ").append(currentItemViewCount.itemId).append(lenBlank + "\t")
        .append("热门度 = ").append(currentItemViewCount.cnt).append("\n")
    }


    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
