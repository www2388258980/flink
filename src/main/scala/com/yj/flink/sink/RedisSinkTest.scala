package com.yj.flink.sink

import com.yj.flink.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    val inputStream = env.readTextFile("G:\\flink\\src\\main\\resources\\hello.txt");
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",");
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble);
      });

    // 定义一个FlinkJedisConfigBase
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build();

    dataStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper));

    env.execute("redis sink test");
  }
}

// 定义一个RedisMapper
class MyRedisMapper extends RedisMapper[SensorReading] {
  // 定义保存数据写入redis命令,HSET 表名 key value
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temp");
  }

  // 将温度值定义为value
  override def getKeyFromData(t: SensorReading): String = t.temperature.toString

  // 将id定义为key
  override def getValueFromData(t: SensorReading): String = t.id
}
