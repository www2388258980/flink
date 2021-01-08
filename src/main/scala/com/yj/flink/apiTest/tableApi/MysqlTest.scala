package com.yj.flink.apiTest.tableApi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table, _}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object MysqlTest {
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
      .groupBy($"id")
      .select($"id", $"id".count() as "cnt");

    // 输出到mysql
    val sinkDDl: String =
      """
        |create table mysqlOutputTable (
        |  id varchar(20) not null,
        |  cnt bigint not null
        |) with (
        |  'connector.type' = 'jdbc',
        |  'connector.url' = 'jdbc:mysql://10.8.37.132:3306/work',
        |  'connector.table' = 'mysqlOutputTable',
        |  'connector.driver' = 'com.mysql.jdbc.Driver',
        |  'connector.username' = 'root',
        |  'connector.password' = 'root'
        |)
      """.stripMargin;

    // 创建表
    tableEnv.executeSql(sinkDDl);
    aggTable.executeInsert("mysqlOutputTable");

    //    env.execute("mysql table test");
  }
}
