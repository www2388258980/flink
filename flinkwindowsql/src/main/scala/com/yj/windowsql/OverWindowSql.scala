package com.yj.windowsql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
  * OVER窗口（OVER Window）是传统数据库的标准开窗，不同于Group By Window，OVER窗口中每1个元素都对应1个窗口。OVER窗口可以按照实际元素的行或实际的元素值（时间戳值）确定窗口，因此流数据元素可能分布在多个窗口中。
  *
  * 在应用OVER窗口的流式数据中，每1个元素都对应1个OVER窗口。每1个元素都触发1次数据计算，每个触发计算的元素所确定的行，都是该元素所在窗口的最后1行。在实时计算的底层实现中，OVER窗口的数据进行全局统一管理（数据只存储1份），逻辑上为每1个元素维护1个OVER窗口，为每1个元素进行窗口计算，完成计算后会清除过期的数据。
  *
  * Flink SQL中对OVER窗口的定义遵循标准SQL的定义语法，传统OVER窗口没有对其进行更细粒度的窗口类型命名划分。按照计算行的定义方式，OVER Window可以分为以下两类：
  * ROWS OVER Window：每1行元素都被视为新的计算行，即每1行都是一个新的窗口。
  * RANGE OVER Window：具有相同时间值的所有元素行视为同一计算行，即具有相同时间值的所有行都是同一个窗口。
  */
object OverWindowSql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build();
    val tableEnv = StreamTableEnvironment.create(env, settings);

    // rows over
    //    rowsOverWindow(tableEnv);


    // range over
    rangeOverWindow(tableEnv);

    env.execute("over window job");
  }

  /**
    * SELECT
    * agg1(col1) OVER(
    * [PARTITION BY (value_expression1,..., value_expressionN)]
    * ORDER BY timeCol
    * ROWS
    * BETWEEN (UNBOUNDED | rowCount) PRECEDING AND CURRENT ROW) AS colName, ...
    * FROM Tab1;
    *
    * UNBOUNDED -- 代表无界流,从开始一直统计到当前元素
    *
    * 要求输出在当前商品上架之前同类的3个商品中的最高价格。
    *
    * @param tableEnv
    */
  def rowsOverWindow(tableEnv: StreamTableEnvironment): Unit = {
    tableEnv.executeSql(
      """
        |create table shop(
        |  itemid     varchar,
        |  itemtype   varchar,
        |  eventtime  varchar,
        |  onselltime as to_timestamp(eventtime),
        |  price      double,
        |  watermark for onselltime as onselltime
        |) with (
        |  'connector.type' = 'filesystem',
        |  'format.type' = 'csv',
        |  'connector.path' = 'G:\flink\flinkwindowsql\src\main\resources\shops.txt'
        |)
      """.stripMargin);

    val result = tableEnv.sqlQuery(
      """
        |select itemid,itemtype,onselltime,price,
        |       max(price) over(partition by itemtype order by onselltime rows between 2 preceding and current row) as maxprice
        |from shop
      """.stripMargin);

    result.toRetractStream[Row].print("rows over");

  }

  /**
    * SELECT
    * agg1(col1) OVER(
    * [PARTITION BY (value_expression1,..., value_expressionN)]
    * ORDER BY timeCol
    * RANGE
    * BETWEEN (UNBOUNDED | timeInterval) PRECEDING AND CURRENT ROW) AS colName,
    * ...
    * FROM Tab1;
    *
    * 需要求比当前商品上架时间早2分钟的同类商品中的最高价格
    *
    * @param tableEnv
    */
  def rangeOverWindow(tableEnv: StreamTableEnvironment): Unit = {
    tableEnv.executeSql(
      """
        |create table shop(
        |  itemid     varchar,
        |  itemtype   varchar,
        |  eventtime  varchar,
        |  onselltime as to_timestamp(eventtime),
        |  price      double,
        |  watermark for onselltime as onselltime
        |) with (
        |  'connector.type' = 'filesystem',
        |  'format.type' = 'csv',
        |  'connector.path' = 'G:\flink\flinkwindowsql\src\main\resources\shops.txt'
        |)
      """.stripMargin);

    val result = tableEnv.sqlQuery(
      """
        |select itemid,itemtype,onselltime,price,
        |       max(price) over(partition by itemtype order by onselltime
        |              range between interval '2' minute preceding and current row) as maxprice
        |from shop
      """.stripMargin);

    result.toRetractStream[Row].print("range over");

  }


}
