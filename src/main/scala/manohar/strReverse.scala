package manohar


object strReverse extends App {

  val str = "hai gopi reddy".toUpperCase
  println(str.reverse)

  var revString = ""
  for (i <- 1 to str.length) {

    var res = str.substring(str.length-i,str.length-(i-1))
    revString+=res

  }
  println(revString)


}
