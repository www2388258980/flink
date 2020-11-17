package com.yj.flink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}

object WordCount1 {
  def main(args: Array[String]): Unit = {
    // 创建批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment;
    // 从文件中读取数据
    val inputPath: String = "G:\\flink\\src\\main\\resources\\hello.txt";
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath);
    // 对数据进行转换统计:先分词,再按照word进行分组最后聚合统计
    val result: DataSet[(String, Int)] = inputDataSet
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0) // 以第一个元素作为key分组
      .sum(1) // 对所有数据的第二个元素求和

    // 打印输出
    result.print();


  }
}
