package com.yj.orderPay_detect

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object OrderPayTimeOut {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // 读取数据，转换类型
    val resource = getClass.getResource("/OrderLog.csv");
    val orderEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val arr = data.split(",");
        OrderEevent(arr(0).toLong, arr(1), arr(2), arr(3).toLong);
      })
      .assignAscendingTimestamps(_.timestap * 1000L)
      .keyBy(_.orderId);

    // 定义模式，应用到流上
    val orderPattern = Pattern
      .begin[OrderEevent]("create").where(_.orderType == "create")
      .followedBy("pay").where(_.orderType == "pay")
      .within(Time.minutes(15));
    val patternStream = CEP.pattern(orderEventStream, orderPattern);

    // 定义侧输出流,用于处理超时事件
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout");

    val resultStream = patternStream
      .select[OrderResult, OrderResult](orderTimeoutOutputTag)((map, timeout) => {
      val timeoutOrderId = map.get("create").iterator.next().head.orderId;
      OrderResult(timeoutOrderId, "timeout: " + timeout);
    })(m => {
      val payedOrderId = m.get("pay").iterator.next().head.orderId;
      OrderResult(payedOrderId, "payed")
    });

    resultStream.print("payed: ");
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout: ");

    env.execute("order timeout job");

  }
}

case class OrderEevent(orderId: Long, orderType: String, txId: String, timestap: Long);

case class OrderResult(orderId: Long, orderMsg: String);
