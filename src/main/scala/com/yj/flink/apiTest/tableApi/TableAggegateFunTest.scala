package com.yj.flink.apiTest.tableApi

import com.yj.flink.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object TableAggegateFunTest {
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

    // table
    val top2Temp = new Top2Temp;
    val resultTable = sensorTable
      .groupBy($"id")
      .flatAggregate(top2Temp($"temperature") as("temp", "rank"))
      .select($"id", $"temp", $"rank");
//    resultTable.toRetractStream[Row].print();

    // sql
//    tableEnv.createTemporaryView("sensor", sensorTable);
    //    tableEnv.registerFunction("top2Temp", top2Temp);
    //    val resultSqlTable = tableEnv.sqlQuery(
    //      """
    //        |select id,top2Temp(temperature)
    //        |from sensor
    //        |group by id
    //      """.stripMargin);
    //
    //    resultSqlTable.toRetractStream[Row].print("sql");


    env.execute("table aggregate func test");

  }
}

class Top2TempAcc {
  var top1: Double = Double.MinValue;
  var top2: Double = Double.MaxValue;
}

class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
  override def createAccumulator(): Top2TempAcc = new Top2TempAcc

  def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
    if (temp > acc.top1) {
      acc.top2 = acc.top1;
      acc.top1 = temp;
    } else if (temp > acc.top2) {
      acc.top2 = temp;
    }
  }

  def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit = {
    if (acc.top1 != Double.MinValue && acc.top2 != Double.MinValue) {
      out.collect(acc.top1, 1);
      out.collect(acc.top2, 2);
    } else if (acc.top2 == Double.MinValue) {
      out.collect(acc.top1, 1);
    }
  }
}
