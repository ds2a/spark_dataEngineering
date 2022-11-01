package com.ds2a.spark.etl.core

import scala.collection.Iterator.empty.duplicate
import scala.collection.mutable.ListBuffer

object  alphaa  extends App {
//list of the elements in the given dataset
  val lst = List(1, 2, 2, 3, 4, 5)


//empty list creation
  var duplicate = List[Int]().toSet
  var withOutDuplicates = List[Int]().toSet

  // i taken 0 elements to last elements
  for (i <- 0 to lst.length - 1) {
    //check the continue with numbers to remaining element in the given dataset
    for (j <- i + 1 to lst.length - 1) {
      //it is possible any element compare with remaining elements what is meaning is
      //where in case i = j then add duplicate
      if (lst(i) == lst(j)) {
        duplicate += lst(i)
      }
        //remaining elements are added to another set
      else
        withOutDuplicates+=lst(i)
    }

  }

  // finally we identify the unique values then we delete the one set to another set
  var unique = withOutDuplicates.diff(duplicate).toSet

// finally we want duplicate values and unique values
  println(duplicate+" :dup")
  println(withOutDuplicates+": withoutdup")
  println(unique+":uni"
  )


}