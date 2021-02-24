package com.yj.orderPay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object TxMatchWithIntervalJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // 读取支付数据
    val resource = getClass.getResource("/OrderLog.csv");
    val orderEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val arr = data.split(",");
        OrderEevent(arr(0).toLong, arr(1), arr(2), arr(3).toLong);
      })
      .assignAscendingTimestamps(_.timestap * 1000L)
      .filter(_.orderType == "pay")
      .keyBy(_.txId);
    // 读取到账数据
    val resource2 = getClass.getResource("/ReceiptLog.csv");
    val receiptEventStream = env.readTextFile(resource2.getPath)
      .map(data => {
        val arr = data.split(",");
        ReceiptEvent(arr(0), arr(1), arr(2).toLong);
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.txId);

    val resultStream = orderEventStream.intervalJoin(receiptEventStream)
      .between(Time.seconds(-3), Time.seconds(5))
      .process(new MyProcessJoinFunction());

    resultStream.print();

    env.execute("实时对账-interval join");
  }
}

class MyProcessJoinFunction extends ProcessJoinFunction[OrderEevent, ReceiptEvent, (OrderEevent, ReceiptEvent)] {
  override def processElement(left: OrderEevent, right: ReceiptEvent,
                              ctx: ProcessJoinFunction[OrderEevent, ReceiptEvent, (OrderEevent, ReceiptEvent)]#Context,
                              out: Collector[(OrderEevent, ReceiptEvent)]): Unit = {
    out.collect((left, right));
  }
}
