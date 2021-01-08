package com.yj.flink.apiTest.tableApi

import com.yj.flink.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{EnvironmentSettings, _}
import org.apache.flink.types.Row

object OverWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build();
    val tableEnv = StreamTableEnvironment.create(env, settings);

    val inputStream = env.readTextFile("G:\\flink\\src\\main\\resources\\hello.txt");
    // 先转换成样例类型
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",");
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble);
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
      });

    val sensorTable = tableEnv.fromDataStream(dataStream, $"id", $"temperature", $"timestamp".rowtime() as "ts");

    // table api
    val resultTable = sensorTable
      .window(Over partitionBy $"id" orderBy $"ts" preceding 2.rows as $"ow")
      .select($"id", $"ts", $"id".count() over $"ow", $"temperature".avg() over $"ow");

    //    resultTable.toAppendStream[Row].print("table");

    // sql
    tableEnv.createTemporaryView("sensor", sensorTable);
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        |  id,ts,count(id) over ow,avg(temperature) over ow
        |from sensor
        |window ow as (
        |  partition By id
        |  order by ts
        |  rows between 2 preceding and current row
        |)
      """.stripMargin);

    resultSqlTable.toAppendStream[Row].print("sql")

    env.execute("over window test");
  }
}
