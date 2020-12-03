package com.yj.flink.apiTest

import java.{lang, util}

import com.yj.flink.source.SensorReading
import com.yj.flink.window.MyReduceFunction
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;

    // 读取数据
    val inputStream = env.socketTextStream("localhost", 7777);
    // 先转换成样例类型
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",");
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble);
      });

    val alterStream = dataStream
      .keyBy(_.id)
      //      .flatMap(new MyFlagMapFunction(10.0));
      .flatMapWithState[(String, Double, Double), Double]({
      case (data: SensorReading, None) => (List.empty, None)
      case (data: SensorReading, lastTemp: Some[Double]) => ({
        if ((data.temperature - lastTemp.get).abs > 10) {
          (List((data.id, lastTemp.get, data.temperature)), Some(data.temperature))
        } else {
          (List.empty, Some(data.temperature))
        }
      })
    });

    alterStream.print();

    env.execute("state test");
  }
}

class MyFlagMapFunction(threadhold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  var lastTempState: ValueState[Double] = _;

  var flag = true;

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
    );
    // 赋予一个不可能的温度值
    //     java.lang.NullPointerException: No key set. This method should not be called outside of a keyed context.
    //    lastTempState.update(-2000.0);
  }

  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    if (flag) {
      flag = false;
    } else {
      // 获取上次的温度值// 获取上次的温度值
      val lastTemp = lastTempState.value
      // 如果差值大于10就报警
      if ((in.temperature - lastTemp).abs > 10) collector.collect(in.id, lastTemp, in.temperature)
    }
    // 更新状态
    lastTempState.update(in.temperature);
  }
}

class MyMapperFunction extends RichMapFunction[SensorReading, String] {

  var valueState: ValueState[Double] = _;
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(
    new ListStateDescriptor[Int]("listState", classOf[Int])
  );
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Double]("mapState", classOf[String], classOf[Double])
  );

  //  lazy val reduceState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(
  //    new ReducingStateDescriptor[SensorReading]("reduceState", new MyReduceFunction[SensorReading], classOf[SensorReading])
  //  );


  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]));
  }

  override def map(in: SensorReading): String = {
    valueState.value();
    valueState.update(1.3);

    listState.add(1);
    val list = new util.ArrayList[Int]();
    list.add(1);
    list.add(2);
    listState.addAll(list);
    listState.update(list);
    val it: lang.Iterable[Int] = listState.get();

    mapState.put("sensor_1", 1.3);
    val it2: util.Iterator[util.Map.Entry[String, Double]] = mapState.iterator();


    return in.id;
  }
}
