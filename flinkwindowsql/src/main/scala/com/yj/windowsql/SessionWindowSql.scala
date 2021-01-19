package com.yj.windowsql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
  * 统计每个用户在每个活跃会话期间的单击次数，会话超时时长为30秒。
  */
object SessionWindowSql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build();
    val tableEnv = StreamTableEnvironment.create(env, settings);

    tableEnv.executeSql(
      """
        |CREATE TEMPORARY TABLE user_clicks(
        |  username  varchar,
        |  click_url varchar,
        |  eventtime varchar,
        |  ts as to_timestamp(eventtime),
        |  watermark for ts as ts - interval '2' second
        |) with (
        |   'connector.type' = 'filesystem',
        |   'format.type' = 'csv',
        |   'connector.path' = 'G:\flink\flinkwindowsql\src\main\resources\click.txt'
        |)
      """.stripMargin);

    val result = tableEnv.sqlQuery(
      """
        |select username,count(click_url) as cnt,
        |       session_start(ts,interval '30' second),
        |       session_end(ts,interval '30' second)
        |from user_clicks
        |group by session(ts,interval '30' second),username
      """.stripMargin);

    result.toRetractStream[Row].print();


    env.execute("session window sql job");
  }
}
