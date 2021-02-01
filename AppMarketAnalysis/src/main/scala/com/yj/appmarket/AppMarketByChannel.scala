package com.yj.appmarket

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random


/**
  * 自定义数据源以及滑动窗口统计
  */
object AppMarketByChannel {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // 开窗统计
    val dataStream = env.addSource(new MarketSource)
      .assignAscendingTimestamps(data => data.timestamp)
      .filter(data => !"uninstall".equals(data.behavior))
      .map(data => (data.channel, data.behavior))
      .keyBy(data => (data._1, data._2))
      .timeWindow(Time.hours(1), Time.seconds(5))
      .process(new MarketProcessWindowFunction); // 同一个分组的会经过process处理

    dataStream.print();

    env.execute("app market analysis");
  }
}

// 定义输入数据样例类
case class MarketUserBeHavior(userId: String, behavior: String, channel: String, timestamp: Long)

// 定义输出数据样例类
case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String,
                           cnt: Long)

class MarketSource extends RichSourceFunction[MarketUserBeHavior] {
  // 是否运行标志
  var running = true;
  // 定义用户行为和渠道标志
  val behaviorSet: Seq[String] = Seq("view", "download", "install", "uninstall");
  val channelSet: Seq[String] = Seq("appstore", "weibo", "wechat", "tieba");
  val random: Random = Random;

  override def run(ctx: SourceFunction.SourceContext[MarketUserBeHavior]): Unit = {
    // 定义一个最大生成数量
    var maxCnt = Long.MaxValue;
    var currentCnt = 0L;
    // 定义while循环，不停的生成数据
    while (running && currentCnt < maxCnt) {
      ctx.collect(
        MarketUserBeHavior(
          UUID.randomUUID().toString,
          behaviorSet(random.nextInt(behaviorSet.size)),
          channelSet(random.nextInt(channelSet.size)),
          System.currentTimeMillis()
        )
      );
      currentCnt += 1;
      Thread.sleep(50L);
    }
  }

  override def cancel(): Unit = running = false
}

// 自定义ProcessWindowFunction
class MarketProcessWindowFunction extends ProcessWindowFunction[(String, String), MarketViewCount, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[(String, String)],
                       out: Collector[MarketViewCount]): Unit = {
    val start = new Timestamp(context.window.getStart).toString;
    val end = new Timestamp(context.window.getEnd).toString;
    val channel = key._1;
    val behavior = key._2;
    val count = elements.size;
    out.collect(MarketViewCount(start, end, channel, behavior, count));
  }
}