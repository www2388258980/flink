package com.yj.appmarket

import java.sql.Timestamp
import java.time.Duration
import java.util.Properties

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector


/**
  * 广告统计分析
  * 黑名单过滤并加入到侧输出流
  */
object AdMarketAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


    val inptuStream = env.socketTextStream("localhost", 7777);

    inptuStream.print("row data");

    val filterStream = inptuStream
      .map(data => {
        val arr = data.split(",");
        AdInputData(arr(0), arr(1), arr(2), arr(3), arr(4).toLong);
      })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(5)) // 周期生成水位线并且延时5秒
          .withTimestampAssigner(new SerializableTimestampAssigner[AdInputData] {
          override def extractTimestamp(element: AdInputData, recordTimestamp: Long): Long = element.timestamp * 1000L
        }))
      // 黑名单过滤,并且加入侧输出流
      .keyBy(data => (data.userId, data.adId))
      .process(new AdBlackKeyedProcessFunction(10));

    val dataStream = filterStream
      .keyBy(_.province)
      .timeWindow(Time.days(1), Time.hours(1))
      .aggregate(new AdAggregateFunction, new AdWindowFunction);

    dataStream.print();
    filterStream.getSideOutput(new OutputTag[AdBlackMenu]("black")).print("warning");

    env.execute("ad analysis job");
  }
}

// 定义输入数据源
case class AdInputData(userId: String, adId: String, province: String, city: String, timestamp: Long)

// 定义输出数据源
case class AdOutputData(windowStart: String, windowEnd: String, province: String, cnt: Long)

// 定义黑名单样例类
case class AdBlackMenu(userId: String, adId: String, msg: String)

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

class AdBlackKeyedProcessFunction(size: Int) extends KeyedProcessFunction[(String, String), AdInputData, AdInputData] {
  // 定义计数状态，时间戳状态，是否加入黑名单状态
  lazy val cntState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("cnt", classOf[Long]));
  lazy val tsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("ts", classOf[Long]));
  lazy val isBlack: ValueState[Boolean] = getRuntimeContext.getState(
    new ValueStateDescriptor[Boolean]("isBlack", classOf[Boolean]));

  override def processElement(value: AdInputData, ctx: KeyedProcessFunction[(String, String), AdInputData, AdInputData]#Context,
                              out: Collector[AdInputData]): Unit = {
    println(new Timestamp(value.timestamp * 1000L).toString);
    var cnt = cntState.value();
    if (cnt == 0) {
      val ts = (value.timestamp * 1000 / (1000 * 60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000;
      //      println("(userId,adId):(" + value.userId + "," + value.adId + ")");
      //      println(new Timestamp(ts).toString);
      tsState.update(ts);
      ctx.timerService().registerEventTimeTimer(ts);
    }
    if (cnt >= size) { // 达到阈值，认为是刷单
      if (!isBlack.value()) { // 没有加入黑名单，输出到侧输出流
        //        println("black")
        ctx.output(new OutputTag[AdBlackMenu]("black"),
          AdBlackMenu(value.userId, value.adId, "Click ad over " + size + " times today."));
        isBlack.update(true);
      }
      return;
    }
    // 正常情况下,加入，输出
    cntState.update(cnt + 1);
    out.collect(value);
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(String, String), AdInputData, AdInputData]#OnTimerContext,
                       out: Collector[AdInputData]): Unit = {
    if (timestamp == tsState.value()) {
      println("ottimee")
      tsState.clear();
      isBlack.clear();
    }
  }
}


