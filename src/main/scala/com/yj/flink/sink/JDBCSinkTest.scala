package com.yj.flink.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.yj.flink.source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object JDBCSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    val inputStream = env.readTextFile("G:\\flink\\src\\main\\resources\\hello.txt");
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",");
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble);
      });

    dataStream.addSink(new MyJDBCSinkFunction());

    env.execute("jdbc sink test");
  }
}

class MyJDBCSinkFunction() extends RichSinkFunction[SensorReading] {

  // 定义连接和预编译语句
  var conn: Connection = _;
  var insertStmt: PreparedStatement = _;
  var updateStmt: PreparedStatement = _;

  override def open(parameters: Configuration): Unit = {
    Class.forName("oracle.jdbc.driver.OracleDriver");
    conn = DriverManager.getConnection("jdbc:oracle:thin:@10.0.11.62:1521/urplatform",
      "urp_jsiadb", "urp_jsiadb_1106");
    insertStmt = conn.prepareStatement("insert into sensor_temp values(?,?)");
    updateStmt = conn.prepareStatement("update sensor_temp set temp=? where id=?");
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    // 先执行更新操作，查到就更新
    updateStmt.setString(1, value.temperature.toString);
    updateStmt.setString(2, value.id);
    updateStmt.execute();
    // 如果没有更新,那就插入
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id);
      insertStmt.setString(2, value.temperature.toString);
      insertStmt.execute();
    }

  }

  override def close(): Unit = {
    insertStmt.close();
    updateStmt.close();
    conn.close();
  }
}
