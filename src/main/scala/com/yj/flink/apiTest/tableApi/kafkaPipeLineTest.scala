package com.yj.flink.apiTest.tableApi


import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object kafkaPipeLineTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    val tableEnv = StreamTableEnvironment.create(env);

    tableEnv.connect(
      new Kafka()
        .version("0.11")
        .topic("producer")
        .property("zookeeper.connect", "localhost:2181")
        .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable");

    val aggTable: Table = tableEnv.from("kafkaInputTable")
      .groupBy("id")
      .select("id,id.count as cnt");

    tableEnv.connect(
      new Kafka()
        .version("0.11")
        .topic("consumer")
        .property("zookeeper.connect", "localhost:2181")
        .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("cnt", DataTypes.BIGINT())
      )
      .createTemporaryTable("kafkaOutputTable");

    //    aggTable.insertInto("kafkaOutputTable");
    // 报错,kafka不支持撤回模式和更新模式
    aggTable.executeInsert("kafkaOutputTable")

    env.execute("table api test");

  }
}
