package com.yj.flink.apiTest.tableApi


import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}


object TableApiTest {
  def main(args: Array[String]): Unit = {
    // 1.创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment;

    val tableEvn = StreamTableEnvironment.create(env);

    /*
    // 1.1基于老版planner的流处理
    val settings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build();
    val oldStreamTableEnv = StreamTableEnvironment.create(env, settings);

    // 1.2基于老版本的批处理
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment;
    val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);

    // 1.3基于blink planner的流处理
    val blinkStreamSettings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build();
    val blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);

    // 1.4基于blink planner的批处理
    val blinkBatchSettings = EnvironmentSettings
      .newInstance()
      .inBatchMode()
      .build();
//    val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);
*/
    // 2.连接外部系统，读取数据，注册表
    // 2.1读取文件
    val filpath = "G:\\flink\\src\\main\\resources\\hello.txt";
    tableEvn.connect(new FileSystem().path(filpath))
      .withFormat(new OldCsv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable");

    // 2.2从kafka读取数据
    tableEvn.connect(
      new Kafka()
        .version("0.11")
        .topic("version")
        .property("zookeeper.connect", "localhost:2181")
        .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    //    val inputTable = tableEvn.from("kafkaInputTable");
    //    inputTable.toAppendStream[(String, Long, Double)].print();

    // 3.查询转换
    // 3.1使用table api
    val sensorTable = tableEvn.from("inputTable")
      .select("id, temperature")
      .filter("id='sensor_1'");
    //.orderBy("temperature.desc"); //A limit operation on unbounded tables is currently not supported.

    // 3.2使用sql
    var resultSqlTable = tableEvn.sqlQuery(
      """
        |select id,temperature
        |from inputTable
        |where id ='sensor_1'
      """.stripMargin);

    sensorTable.toAppendStream[(String, Double)].print("table");
    resultSqlTable.toAppendStream[(String, Double)].print("sql");

    env.execute("table api test");
  }
}
