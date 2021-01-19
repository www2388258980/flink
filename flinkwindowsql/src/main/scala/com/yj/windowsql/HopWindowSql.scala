package com.yj.windowsql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
  * 统计每个用户过去1分钟的单击次数，每30秒更新1次
  * HOP窗口无法读取数据进入的时间，第一个窗口的开启时间会前移。前移时长=窗口时长-滑动步长，示例如下表。
  * 窗口时长（秒） 滑动步长（秒）   Event Time             第一个窗口StartTime       第一个窗口EndTime
  * 120          30            2019-07-31 10:00:00.0    2019-07-31 09:58:30.0    2019-07-31 10:00:30.0
  * 60           10            2019-07-31 10:00:00.0    2019-07-31 09:59:10.0    2019-07-31 10:00:10.0
  */
object HopWindowSql {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    //    env.setParallelism(1);

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build();
    val tableEnv = StreamTableEnvironment.create(env, settings);

    // 滑动窗口,事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    hopEnventime(tableEnv);

    env.execute("hop window sql job");

  }


  def hopEnventime(tableEnv: StreamTableEnvironment): Unit = {
    tableEnv.executeSql(
      """
        |CREATE TEMPORARY TABLE user_clicks(
        |  username  varchar,
        |  click_url varchar,
        |  eventtime varchar,
        |  ts as TO_TIMESTAMP(eventtime),
        |  WATERMARK FOR ts AS ts - INTERVAL '2' SECOND  -- 为Rowtime定义Watermark
        |) with (
        |  'connector.type' = 'filesystem',
        |  'format.type' = 'csv',
        |  'connector.path' = 'G:\flink\flinkwindowsql\src\main\resources\click.txt'
        |)
      """.stripMargin);
    val result = tableEnv.sqlQuery(
      """
        |select username,count(username) as cnt,
        |       hop_start(ts,interval '30' SECOND,interval '1' minute),
        |       hop_end(ts,interval '30' SECOND,interval '1' minute)
        |from user_clicks
        |group by hop(ts,interval '30' SECOND,interval '1' minute),username
      """.stripMargin);

    result.toRetractStream[Row].print("hop");
  }
}
