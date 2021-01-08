package com.yj.flink.apiTest.tableApi

import com.yj.flink.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Table, _}
import org.apache.flink.table.api.bridge.scala._

object Example {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    val inputStream = env.readTextFile("G:\\flink\\src\\main\\resources\\hello.txt");
    // 先转换成样例类型
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",");
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble);
      });

    // 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env);
    // 基于流创建一张表
    val dataTable: Table = tableEnv.fromDataStream(dataStream);


    // 调用table api 进行转换
    val resultTable = dataTable
      .select('id, 'temperature)
      .filter('id === "sensor_1");

    resultTable.toAppendStream[(String, Double)].print("result");


    // 直接用sql实现
    tableEnv.createTemporaryView("dataTable", dataTable);
    val sql: String = "select id,temperature from dataTable where id='sensor_1'";
    val resultSqlTable = tableEnv.sqlQuery(sql);

    resultSqlTable.toAppendStream[(String, Double)].print("resultSql");


    env.execute("table api example")
  }
}
