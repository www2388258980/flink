package com.yj.flink.apiTest.tableApi

import com.yj.flink.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.{TableFunction}
import org.apache.flink.types.Row

object UdfTableFuncTest {
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
    val spit = new Split("_");
    val resultTable = sensorTable
      .joinLateral(spit($"id") as("word", "length"))
      .select($"id", $"ts", $"word", $"length");

    //    resultTable.toAppendStream[Row].print();

    // sql 注册标量函数
    tableEnv.createTemporaryView("sensor", sensorTable);
    tableEnv.registerFunction("split", spit);
    // 报错
    //    tableEnv.createTemporarySystemFunction("split", spit);
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id,ts,word,length
        |from sensor,lateral table ( split(id) ) as splitid(word,length)
      """.stripMargin);
    resultSqlTable.toAppendStream[Row].print("sql");


    env.execute("udf table func test");
  }
}

// 自定义表函数
class Split(seperator: String) extends TableFunction[(String, Int)] {
  def eval(s: String): Unit = {
    s.split(seperator).foreach(
      word => collect((word, word.length))
    )
  }
}
