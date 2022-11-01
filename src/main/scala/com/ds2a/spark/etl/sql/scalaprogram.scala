package com.ds2a.spark.etl.sql
import scala.io.StdIn.readLine
object scalaprogram {
  def main(args: Array[String]):Unit = {
    var x = readLine("Enter Age of a player: ");
    var age = x.toInt
    if(age > 13){
      println("Allowed To PlayGround")
    }
    else{
      println("Not Allowed To PlayGround")
    }
  }
  }
