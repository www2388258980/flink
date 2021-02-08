package com.yj.login_fail_detect

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 恶意登录检测
  */
object LoginFailDetect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    val dataStream = env.readTextFile(getClass.getResource("/LoginLog.csv").getPath)
      .map(m => {
        val arr = m.split(",");
        LoginEvent(arr(0), arr(1), arr(2), arr(3).toLong);
      })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(3)) // 周期生成,水位线延迟3秒
          .withTimestampAssigner(new SerializableTimestampAssigner[LoginEvent] {
          override def extractTimestamp(element: LoginEvent, recordTimestamp: Long): Long = element.timestap * 1000L
        }))
      .keyBy(k => k.userId) // 根据用户分组
      //      .process(new LoginFailKeyedProcessFunction(2));
      .timeWindow(Time.seconds(2), Time.seconds(1))
      .process(new LonginFailProcessWindowFunction(2))

    dataStream.print();

    env.execute("login fail detect");
  }
}

// 输入样例类
case class LoginEvent(userId: String, ip: String, eventType: String, timestap: Long)

// 输出样例类
case class WarningOutput(userId: String, firstTime: Long, lastTime: Long, warningMsg: String)

class LonginFailProcessWindowFunction(size: Int) extends ProcessWindowFunction[LoginEvent, WarningOutput, String, TimeWindow] {

  override def process(key: String, context: Context, elements: Iterable[LoginEvent], out: Collector[WarningOutput]): Unit = {
    // 如果存在成功数据,则不输出
    var flag = false;
    elements.foreach(f => {
      if ("success".equals(f.eventType)) {
        flag = true;
      }
    })
    if (!flag && elements.size >= size) { // 没有成功数据，都是失败数据
      out.collect(WarningOutput(
        elements.head.userId,
        elements.head.timestap,
        elements.last.timestap,
        "用户" + elements.head.userId + "在2秒内" + elements.size + "次登录失败!!!"
      ))
    }
  }
}

// 自定义登录失败处理类
class LoginFailKeyedProcessFunction(size: Int) extends KeyedProcessFunction[String, LoginEvent, WarningOutput] {
  // 定义保存登录失败状态和定时器时间戳状态
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext
    .getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]));
  lazy val tsState: ValueState[Long] = getRuntimeContext
    .getState(new ValueStateDescriptor[Long]("ts", classOf[Long]));

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[String, LoginEvent, WarningOutput]#Context,
                              out: Collector[WarningOutput]): Unit = {
    if ("fail".equals(value.eventType)) {
      val ts = value.timestap * 1000L;
      if (tsState.value() == 0) {
        ctx.timerService().registerEventTimeTimer(ts);
        tsState.update(ts)
      }
      loginFailListState.add(value);
    } else if ("success".equals(value.eventType)) {
      ctx.timerService().deleteEventTimeTimer(tsState.value());
      tsState.clear();
      loginFailListState.clear();
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, LoginEvent, WarningOutput]#OnTimerContext,
                       out: Collector[WarningOutput]): Unit = {
    val buff: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent];
    val iter = loginFailListState.get().iterator();
    while (iter.hasNext) {
      buff += iter.next();
    }
    if (buff.size >= size) {
      out.collect(WarningOutput(
        buff.head.userId,
        buff.head.timestap,
        buff.last.timestap,
        buff.head.userId + "在2秒内登录" + buff.size + "次失败"
      ))
    }
    loginFailListState.clear();
    tsState.clear();
  }
}