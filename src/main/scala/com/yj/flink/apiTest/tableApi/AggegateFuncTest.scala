package com.yj.flink.apiTest.tableApi

import com.yj.flink.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, _}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

object AggegateFuncTest {
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

    // table sql
    val avgTemp = new AvgTemp;
    val resultTable = sensorTable
      .groupBy($"id")
      .aggregate(avgTemp($"temperature") as "avgTemp")
      .select($"id", $"avgTemp");
    //    resultTable.toRetractStream[Row].print();

    // sql
    tableEnv.createTemporaryView("sensor", sensorTable);
    tableEnv.registerFunction("avgTemp", avgTemp);
    val sqlResultTable = tableEnv.sqlQuery(
      """
        |select id,avgTemp(temperature) tt
        |from sensor
        |group by id
      """.stripMargin);
    sqlResultTable.toRetractStream[Row].print("sql");


    env.execute("aggegate func test");
  }
}

class AvgTempAcc {
  var sum: Double = 0.0;
  var cnt: Int = 0;
}

class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {
  override def getValue(accumulator: AvgTempAcc): Double = accumulator.sum / accumulator.cnt

  override def createAccumulator(): AvgTempAcc = new AvgTempAcc

  def accumulate(accumulator: AvgTempAcc, temp: Double): Unit = {
    accumulator.sum += temp;
    accumulator.cnt += 1;
  }

}