package com.yj.flink.apiTest.tableApi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, Elasticsearch, FileSystem, Json, Schema}

object EsTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    val tableEnv = StreamTableEnvironment.create(env);

    val filpath = "G:\\flink\\src\\main\\resources\\hello.txt"
    tableEnv.connect(new FileSystem().path(filpath)).
      withFormat(new Csv()).
      withSchema(new Schema()
        .field("id", DataTypes.STRING)
        .field("timestamp", DataTypes.BIGINT)
        .field("temperature", DataTypes.DOUBLE)
      ).createTemporaryTable("inputTable");

    val aggTable: Table = tableEnv.from("inputTable")
      .groupBy("id")
      .select("id,id.count as cnt");

    // 输出到elasticSearch
    tableEnv.connect(
      new Elasticsearch()
        .version("6")
        .host("127.0.0.1", 9200, "hhtp")
        .index("sensor")
        .documentType("temperature")
    )
      .inUpsertMode()
      .withFormat(new Json())
      .withSchema(
        new Schema()
          .field("id", DataTypes.STRING())
          .field("cnt", DataTypes.BIGINT())
      )
      .createTemporaryTable("esOutputTable");

    aggTable.executeInsert("esOutputTable");

    env.execute("es test");

  }
}
