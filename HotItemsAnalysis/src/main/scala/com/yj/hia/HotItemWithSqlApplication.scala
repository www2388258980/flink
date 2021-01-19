package com.yj.hia

import com.yj.hia.pojo.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

object HotItemWithSqlApplication {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    env.setParallelism(1)
    // 从文件中读取数据，并转换成样例类
    val inputStream = env.readTextFile("G:\\flink\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");
    val dataStream: DataStream[UserBehavior] = inputStream
      .map((data: String) => {
        val arr = data.split(",");
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong);
      })
      .assignAscendingTimestamps(_.timestamp * 1000L);

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build();
    val tableEnv = StreamTableEnvironment.create(env, settings);

    val dataTable = tableEnv.fromDataStream(dataStream, $"itemId", $"behavior", $"timestamp".rowtime() as "ts");

    // 1. 使用TableApi进行开窗聚合统计
    val aggTable = dataTable
      .filter($"behavior" === "pv")
      .window(Slide over 1.hour() every 5.minute() on $"ts" as "sw")
      .groupBy($"itemId", $"sw")
      .select($"itemId", $"sw".start() as "windowstart", $"sw".end() as "windowEnd", $"itemId".count() as "cnt");
    // 用sql实现topn的选取
    tableEnv.createTemporaryView("aggtable", aggTable, $"itemId", $"windowstart", $"windowEnd", $"cnt");
    val result1 = tableEnv.sqlQuery(
      """
        |select itemId,windowstart,windowEnd,cnt,rn
        |from (
        |   select *,row_number() over (partition by windowEnd order by cnt desc) as rn
        |   from aggtable
        |)
        |where rn <=5
      """.stripMargin);
    //    result1.toRetractStream[Row].print();

    //    aggTable.toRetractStream[Row].print();
    // 使用纯sql实现
    tableEnv.createTemporaryView("datatable", dataStream, $"itemId", $"behavior", $"ts".rowtime());
    val result2 = tableEnv.sqlQuery(
      """
        |select itemId,windowstart,windowEnd,cnt,rn
        |from (
        |   select *,row_number() over (partition by windowEnd order by cnt desc) as rn
        |   from (
        |     select itemId,count(itemId) as cnt,HOP_END(ts,interval '5' minute,INTERVAL  '1' hour) as windowEnd,
        |            HOP_start(ts,interval '5' minute,INTERVAL  '1' hour) as windowstart
        |     from datatable
        |     where behavior='pv'
        |     group by itemId,hop(ts,interval '5' minute,INTERVAL  '1' hour)
        |   )
        |)
        |where rn <=5
      """.stripMargin);

    result2.toRetractStream[Row].print("sql");

    env.execute("hot item with sql job");
  }
}
