package com.yj.networklfow.impl

import com.yj.networklfow.pojo.ApacheLogEnvent
import org.apache.flink.api.common.functions.AggregateFunction

class PageCountAgg extends AggregateFunction[ApacheLogEnvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEnvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
