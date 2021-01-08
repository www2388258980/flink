package com.yj.flink.apiTest.tableApi

import com.yj.flink.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object UdfScalarFuncTest {
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
      });

    val sensorTable = tableEnv.fromDataStream(dataStream, $"id", $"timestamp" as "ts", $"temperature");

    // table api
    val hashCode = new HashCode(20);
    val resultTable = sensorTable.select($"id", $"ts", hashCode($"id"));

    resultTable.toAppendStream[Row].print();

    // sql 注册标量函数
    tableEnv.createTemporaryView("sensor", sensorTable);
    tableEnv.createTemporarySystemFunction("hashcode", hashCode);
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id,ts,hashcode(id)
        |from sensor
      """.stripMargin);
    resultSqlTable.toAppendStream[Row].print("sql");


    env.execute("udf scalar func test");
  }
}

// 自定义标量函数
class HashCode(factor: Int) extends ScalarFunction {
  def eval(s: String): Int = {
    s.hashCode * factor + 1
  }
}
