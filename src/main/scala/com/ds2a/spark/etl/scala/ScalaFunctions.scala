package com.ds2a.spark.etl.scala
object ScalaFunctions {
  def main(args: Array[String]): Unit = {
    def greetingsForKids(name: String, age: Int): String = {
      "Hi, my name is " + name + " and I am " + age + " years old"
    }

    println(greetingsForKids("David", 12))
//Using recursive nature ,Codes for following
// Factorial
    def factorial(n:Int):Int = {
      if(n<=0) 1
      else n*factorial(n-1) //Recursive
    }
    println(factorial(5))
// fibonacci series
    def fibonacci(n:Int):Int =
      if (n<= 2) 1
      else fibonacci(n-1)+fibonacci(n-2) //Recursive

    //1 1 2 3 5 8 13 21......
    println(fibonacci(5))
// Prime Number
    def isPrime(n:Int):Boolean ={
      // Auxilary Function
      def isPrimeUntil(t:Int):Boolean =
        if(t<= 1) true
        else n % t != 0 && isPrimeUntil(t-1) //Recursive

      isPrimeUntil(n/2)
    }
    println(isPrime(37))

  }
}