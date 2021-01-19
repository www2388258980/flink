package com.yj.windowsql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
  * 滚动窗口纯sql用法
  */
object TumbleWindowSql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build();
    val tableEnv = StreamTableEnvironment.create(env, settings);
    // 滚动窗口,事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    enventimeFunction(tableEnv);
    // 滚动窗口,处理时间
    //    processtimeFunction(tableEnv);

    env.execute("tumble job");
  }

  /**
    * 使用Event Time统计每个用户每分钟在指定网站的单击数示例
    *
    * @param tableEnv
    */
  def enventimeFunction(tableEnv: StreamTableEnvironment): Unit = {
    val ddl: String =
      """
        |CREATE TEMPORARY TABLE user_clicks(
        |  username  varchar,
        |  click_url varchar,
        |  eventtime varchar,
        |  ts AS TO_TIMESTAMP(eventtime),
        |  WATERMARK FOR ts AS ts - INTERVAL '2' SECOND  -- 为Rowtime定义Watermark。
        |) with (
        |  'connector.type' = 'filesystem',
        |  'format.type' = 'csv',
        |  'connector.path' = 'G:\flink\flinkwindowsql\src\main\resources\click.txt'
        |)
      """.stripMargin;
    val tableResult = tableEnv.executeSql(ddl);
    tableResult.print();

    val re = tableEnv.sqlQuery(
      """
        |select * from user_clicks
      """.stripMargin);
    re.toAppendStream[Row].print("re");

    val result = tableEnv.sqlQuery(
      """
        |SELECT
        | TUMBLE_START(ts, INTERVAL '1' MINUTE) as window_start,
        | TUMBLE_END(ts, INTERVAL '1' MINUTE) as window_end,
        | username,
        | COUNT(click_url) as cnt
        |FROM user_clicks
        |GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE),username
      """.stripMargin);

    //    result.printSchema();

    result.toRetractStream[Row].print("tunble eventime job");
  }

  /**
    * 使用Processing Time统计每个用户每分钟在指定网站的单击数示例
    *
    * @param tableEnv
    */
  def processtimeFunction(tableEnv: StreamTableEnvironment): Unit = {
    tableEnv.executeSql(
      """
        |CREATE TEMPORARY TABLE user_clicks(
        |  username  varchar,
        |  click_url varchar,
        |  eventtime varchar,
        |  ts as PROCTIME()
        |) with (
        |  'connector.type' = 'filesystem',
        |  'format.type' = 'csv',
        |  'connector.path' = 'G:\flink\flinkwindowsql\src\main\resources\click.txt'
        |)
      """.stripMargin);
    val re = tableEnv.sqlQuery(
      """
        |select * from user_clicks
      """.stripMargin);
    re.toAppendStream[Row].print("re");
    val result = tableEnv.sqlQuery(
      """
        |SELECT
        | TUMBLE_START(ts, INTERVAL '1' MINUTE),
        | TUMBLE_END(ts, INTERVAL '1' MINUTE),
        | username,
        | COUNT(click_url)
        |FROM user_clicks
        |GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), username
      """.stripMargin);

    result.printSchema();

    result.toRetractStream[Row].print();
  }
}
