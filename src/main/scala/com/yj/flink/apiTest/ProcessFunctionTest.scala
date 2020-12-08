package com.yj.flink.apiTest

import com.yj.flink.source.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;

    // 读取数据
    val inputStream = env.socketTextStream("localhost", 7777);
    // 先转换成样例类型
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",");
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble);
      });

    val resultStream = dataStream
      .keyBy(record => record.id)
      .process(new MyKeyedProcessFunction());

    val tempChangeStream = dataStream
      .keyBy(_.id)
      .process(new TempChangeWarningFunction(10000L));


    tempChangeStream.print();

    env.execute("processFunction test")
  }
}

class TempChangeWarningFunction(interval: Long) extends KeyedProcessFunction[String, SensorReading, String] {
  // 定义两个状态,一个保存上一次的温度，一个保存定时器时间戳
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("last-temp", classOf[Double])
  );
  lazy val timeTsState: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("time-ts", classOf[Long])
  );

  var flag = true;

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                              out: Collector[String]): Unit = {
    val lasttemp = lastTempState.value();
    val ts = timeTsState.value();
    // 如果当前温度比上一次温度高，就注册一个当前时间+interval的定时器
    val timeService = ctx.timerService();
    if (flag) {
      lastTempState.update(value.temperature);
      flag = false;
    } else if (ts == 0 && value.temperature > lasttemp) {
      val time = timeService.currentProcessingTime() + interval;
      timeService.registerProcessingTimeTimer(time);
      // 保存定时器
      timeTsState.update(time);
    } else if (value.temperature < lasttemp) {
      // 温度下降,删除定时器
      timeTsState.clear();
    }
    // 更新温度状态值
    lastTempState.update(value.temperature);
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    // 定时器任务，输出报警
    out.collect("温度传感器" + ctx.getCurrentKey + "在" + interval / 1000 + "秒间隔内温度值连续上升");
    // 清空定时器时间戳状态
    timeTsState.clear();
  }
}

class MyKeyedProcessFunction extends KeyedProcessFunction[String, SensorReading, String] {


  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                              out: Collector[String]): Unit = {

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    ctx.getCurrentKey;
    ctx.timestamp();
    ctx.timerService().currentWatermark();
    ctx.timerService().registerEventTimeTimer(timestamp + 60 * 1000L);

  }
}
