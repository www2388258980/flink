package com.yj.flink.apiTest.tableApi

import com.yj.flink.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

object File2FileTest {
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
      .keyBy(_.id)
      .maxBy("id");

    dataStream.print();

    val sensorTable = tableEnv
      .fromDataStream(dataStream, $"id", $"timestamp" as "ts", $"temperature");

    // 连接到外部文件系统ddl
    val fileDDL: String =
      """
        |create table sensor(
        | id varchar(20) not null,
        | ts bigint not null,
        | temperature decimal(5,2)
        |) with (
        |  'connector.type' = 'filesystem',
        |  'format.type' = 'csv',
        |  'connector.path' = 'G:\flink\src\main\resources\sensor.txt'
        |)
      """.stripMargin;
    tableEnv.executeSql(fileDDL);
    sensorTable.executeInsert("sensor");

    env.execute("file to file test");

  }
}
