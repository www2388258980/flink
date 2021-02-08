package com.yj.login_fail_detect

import java.time.Duration
import java.util

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailDetectWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    val dataStream = env.readTextFile(this.getClass.getResource("/LoginLog.csv").getPath)
      .map(map => {
        val arr = map.split(",");
        LoginEvent(arr(0), arr(1), arr(2), arr(3).toLong);
      })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(3))
          .withTimestampAssigner(new SerializableTimestampAssigner[LoginEvent] {
            override def extractTimestamp(element: LoginEvent, recordTimestamp: Long): Long =
              element.timestap * 1000L
          }));

    // 1. 定义匹配模式，要求一个登录失败事件，紧跟着另外一个失败事件
    val loginFailPattern = Pattern
      .begin[LoginEvent]("firstFail").where(_.eventType == "fail")
      .next("secondFail").where(_.eventType == "fail")
      .within(Time.seconds(2));

    // 2. 将模式应用到流中
    val patternStream = CEP.pattern(dataStream.keyBy(_.userId), loginFailPattern);

    // 3. 检出符合模式的流，需要调用select
    val loginFailWarningStream = patternStream.select(new LoginFailEventMatch());

    loginFailWarningStream.print();

    env.execute("login fail detect with cep");

  }
}

class LoginFailEventMatch extends PatternSelectFunction[LoginEvent, WarningOutput] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): WarningOutput = {

    val firstFailEvent = map.get("firstFail").get(0);
    val secondFailEvent = map.get("secondFail").get(0);

    WarningOutput(firstFailEvent.userId, firstFailEvent.timestap, secondFailEvent.timestap, "login fail")
  }
}
