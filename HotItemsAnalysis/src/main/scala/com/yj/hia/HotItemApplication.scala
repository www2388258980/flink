package com.yj.hia

import com.yj.hia.impl.{CountAgg, ItemViewWindowResult, TopNKeyedProcessFunction}
import com.yj.hia.pojo.{ItemViewCount, UserBehavior}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 统计近1小时的热门商品，每5分钟更新一次
  * 热门度用浏览次数("pv")来衡量
  */
object HotItemApplication {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // 从文件中读取数据，并转换成样例类
    val inputStream = env.readTextFile("G:\\flink\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");
    val dataStream: DataStream[UserBehavior] = inputStream
      .map((data: String) => {
        val arr = data.split(",");
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong);
      })
      .assignAscendingTimestamps(_.timestamp * 1000L);

    // 得到窗口聚合结果
    val windowResult: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5)) // 设置滑动窗口进行统计
      .aggregate(new CountAgg(), new ItemViewWindowResult());


    // 根据windowEnd进行分组然后统计top 5
    // T - ItemViewCount K - Long  R - String
    val top5Result: DataStream[String] = windowResult
      .keyBy(_.windowEnd)
      .process(new TopNKeyedProcessFunction(5));

    top5Result.print();

    env.execute("hot item  test ");
  }
}






