package com.yj.flink.transform

import org.apache.flink.streaming.api.scala._

object MinByTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;

    val inputStream = env.readTextFile("G:\\flink\\src\\main\\resources\\person.txt");
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",");
        Person(arr(0), arr(1).toInt, arr(2));
      })
      .keyBy(_.name)
      .min(1);
    dataStream.print("min");

    val dataStream2 = inputStream
      .map(data => {
        val arr = data.split(",");
        Person(arr(0), arr(1).toInt, arr(2));
      })
      .keyBy(_.name)
      .minBy(1);
    dataStream2.print("minby");

    env.execute("min and minBy test");

  }
}

// 定义人员样例类
case class Person(name: String, age: Int, address: String)
