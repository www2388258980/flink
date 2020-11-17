package com.yj.flink.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

// 定义样例类,温度传感器
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    // 1.从集合中读取数据
    val dataList = List(
      SensorReading("sensor_1", 1547718199, 15.8),
      SensorReading("sensor_2", 1547718299, 25.8),
      SensorReading("sensor_3", 1547718399, 6.8),
      SensorReading("sensor_4", 1547718499, 38.8)
    );
    val stream1 = env.fromCollection(dataList);
    stream1.print()

    // 从文件中读取
    val stream2 = env.readTextFile("G:\\flink\\src\\main\\resources\\hello.txt");


    // 从kafka读取数据
    val proper = new Properties();
    proper.setProperty("bootstrap.servers", "localhost:9092");
    proper.setProperty("group.id", "consumer-group");
    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), proper));

    // 自定义source
    val stream4 = env.addSource(new MySensorSource);

    stream4.print();

    // 执行
    env.execute("source test")
  }
}

// 自定义SourceFunction
class MySensorSource() extends SourceFunction[SensorReading] {
  // 定义一个标志位flag,用来表示源数据正常运行发出数据
  var flag = true;

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 定义一个随机数发生器
    val random = new Random();
    // 随机生成一组(10)传感器的初始温度
    var curtemp = 1.to(10).map(i => ("sensor_" + i, random.nextDouble() * 100));
    // 定义无限循环
    while (flag) {
      // 在上次的基础上微调,更新温度值
      curtemp = curtemp.map(data => (data._1, data._2 + random.nextGaussian()));
      // 获取当前时间戳,加入到数据中
      val curtime = System.currentTimeMillis();
      curtemp.foreach(
        data => sourceContext.collect(SensorReading(data._1, curtime, data._2))
      );
      Thread.sleep(1000);
    }
  }

  override def cancel(): Unit = flag = false;
}
