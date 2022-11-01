package com.ds2a.spark.etl.scala

object Factorial {

  def factorial(a: Int): Int = {
    var resultValue = 1
    for (i <- 1 to a) {
      resultValue = resultValue * i;
    }
    return resultValue
  }

  def main(args: Array[String]) {
    println(factorial(7))
  }
}
