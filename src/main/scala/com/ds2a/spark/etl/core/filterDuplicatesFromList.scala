package com.ds2a.spark.etl.core

object filterDuplicatesFromList extends App {

  val lst = List(1, 2, 3, 1)

  var duplicate = List[Int]().toSet
  var withOutDuplicates = List[Int]().toSet
//  var unique = withOutDuplicates-duplicate.toSet

  for (i <- 0 to lst.length - 1) {
    for (j <- i + 1 to lst.length - 1) {
      if (lst(i) == lst(j)) {
        duplicate += lst(i)
      }
      else
        withOutDuplicates+=lst(i)

    }

  }
  var unique = withOutDuplicates.diff(duplicate).toSet
  println(duplicate)
  println(unique)

}
