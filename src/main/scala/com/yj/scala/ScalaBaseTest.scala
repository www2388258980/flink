package com.yj.scala

object ScalaBaseTest {
  def main(args: Array[String]): Unit = {
    1 + 1;
    val two = 1 + 1;
    var name = "steve";
    name = "marius";
    // 匿名函数
    val res2 = (x: Int) => x + 1;
    // 部分应用（Partial application）
    def adder(m: Int, n: Int) = {
      println(m);
      println(n);
      m + n
    }

    val add2 = adder(2, _: Int)
    println(add2(3));
    // 柯里化函数
    println(multiply(2)(3));
    val timesTwo = multiply(2) _;
    timesTwo(3)
  }

  def addOne(m: Int): Int = m + 1

  def multiply(m: Int)(n: Int): Int = m * n

  def capitalizeAll(args: String*) = {
    args.map { arg =>
      arg.capitalize
    }
  }
}

class Calculator(brand: String) {
  /**
    * A constructor.
    */
  val color: String = if (brand == "TI") {
    "blue"
  } else if (brand == "HP") {
    "black"
  } else {
    "white"
  }
  // An instance method.
  def add(m: Int, n: Int): Int = m + n
}
