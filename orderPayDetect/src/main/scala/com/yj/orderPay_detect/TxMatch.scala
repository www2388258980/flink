package com.yj.orderPay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TxMatch {
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
    // 合并流
    val resultStream = orderEventStream.connect(receiptEventStream)
      .process(new TxPayMatchResult());

    resultStream.print("match");
    resultStream.getSideOutput(new OutputTag[OrderEevent]("unmatchedPay")).print("unmatchedPay");
    resultStream.getSideOutput(new OutputTag[ReceiptEvent]("unmatchedRecipt")).print("unmatchedRecipt");

    env.execute("实时对账");
  }
}


case class ReceiptEvent(txId: String, channle: String, timestamp: Long)

class TxPayMatchResult extends CoProcessFunction[OrderEevent, ReceiptEvent, (OrderEevent, ReceiptEvent)] {

  // 定义支付和到账状态
  lazy val payEventState: ValueState[OrderEevent] = getRuntimeContext
    .getState(new ValueStateDescriptor[OrderEevent]("pay", classOf[OrderEevent]));
  lazy val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext
    .getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]));
  // 定义不匹配侧输出流
  val unMatchedPayedOutputTag = new OutputTag[OrderEevent]("unmatchedPay");
  val unMatchedReceiptOutputTag = new OutputTag[ReceiptEvent]("unmatchedRecipt");

  override def processElement1(pay: OrderEevent, ctx: CoProcessFunction[OrderEevent, ReceiptEvent, (OrderEevent, ReceiptEvent)]#Context,
                               out: Collector[(OrderEevent, ReceiptEvent)]): Unit = {
    val receipt = receiptEventState.value();
    // pay事件已经到达，判断receipt事件是否到达
    if (receipt != null) {
      out.collect((pay, receipt));
      receiptEventState.clear();
      payEventState.clear();
    } else {
      // 没有到达，定义定时器进行等待
      ctx.timerService().registerEventTimeTimer(pay.timestap * 1000L + 5000L);
      payEventState.update(pay);
    }
  }

  override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEevent, ReceiptEvent, (OrderEevent, ReceiptEvent)]#Context,
                               out: Collector[(OrderEevent, ReceiptEvent)]): Unit = {
    val pay = payEventState.value();
    // receipt事件已经到达，判断pay事件是否到达
    if (pay != null) {
      out.collect((pay, receipt));
      receiptEventState.clear();
      payEventState.clear();
    } else {
      // 没有到达，定义定时器进行等待
      ctx.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 3000L);
      receiptEventState.update(receipt);
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEevent, ReceiptEvent, (OrderEevent, ReceiptEvent)]#OnTimerContext,
                       out: Collector[(OrderEevent, ReceiptEvent)]): Unit = {
    val receipt = receiptEventState.value;
    val pay = payEventState.value;
    if (receipt != null) {
      ctx.output(unMatchedReceiptOutputTag, receipt)
    } else if (pay != null) {
      ctx.output(unMatchedPayedOutputTag, pay)
    }

    receiptEventState.clear();
    payEventState.clear();
  }
}