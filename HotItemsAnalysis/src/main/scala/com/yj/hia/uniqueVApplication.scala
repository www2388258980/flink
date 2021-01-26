package com.yj.hia

import com.yj.hia.pojo.UserBehavior
import com.yj.hia.util.UvCountResult
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 网站独立访客数统计
  */
object uniqueVApplication {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // 从文件中读取数据，并转换成样例类
    val inputStream = env.readTextFile(getClass.getResource("/UserBehavior.csv").getPath);
    val dataStream: DataStream[UserBehavior] = inputStream
      .map((data: String) => {
        val arr = data.split(",");
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong);
      })
      .assignAscendingTimestamps(_.timestamp * 1000L);

    val uvStream = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1)) // 直接不分组，基于DataStream开1小时滚动窗口
      .apply(new UvCountResult());

    uvStream.print();


    env.execute("unique visitor job");
  }
}
