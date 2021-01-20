package com.yj.networklfow

import java.text.SimpleDateFormat
import java.time.Duration

import com.yj.networklfow.impl.{PageCountAgg, PageViewCountWindowResult, TopNPageKeyedProcessFunction}
import com.yj.networklfow.pojo.ApacheLogEnvent
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object NetworkFlowAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    val dataStream = env.readTextFile("G:\\flink\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
      .map(data => {
        val arr = data.split(" ");
        val ts = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(arr(3)).getTime;
        ApacheLogEnvent(arr(0), arr(1), ts, arr(5), arr(6));
      })
      .filter(data => {
        val pattern = "^((?!\\.(css|js|ico|png|gif|svg|html|ttf|jpg)$).)*$".r;
        (pattern findFirstIn data.url).nonEmpty
      })
      .assignTimestampsAndWatermarks( // 定义哪个属性作为事件时间以及水位线生成规则(周期性生成,并且延时1分钟
        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1))
          .withTimestampAssigner(new SerializableTimestampAssigner[ApacheLogEnvent] {
            override def extractTimestamp(element: ApacheLogEnvent, recordTimestamp: Long): Long = element.timestamp
          })
      );

    //        dataStream.print("dataStream");

    // 进行开窗聚合，以及排序输出
    val aggresult = dataStream
      .filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1)) // 窗口延迟1分钟关闭,意思是窗口关闭时间小于水位线会执行一次，但是不会关闭，会继续等到1分钟，等到数据会在执行一次
      .sideOutputLateData(new OutputTag[ApacheLogEnvent]("late")) // 当一个数据属于的最大时间窗口也关闭的时候，这个数据才会进入侧输出流
      .aggregate(new PageCountAgg(), new PageViewCountWindowResult());

    //    aggresult.print("aggresult");

    val result = aggresult
      .keyBy(_.windowEnd)
      .process(new TopNPageKeyedProcessFunction(3));


    aggresult.getSideOutput(new OutputTag[ApacheLogEnvent]("late")).print("late");

    result.print();

    env.execute("network flow analysis job");
  }
}
