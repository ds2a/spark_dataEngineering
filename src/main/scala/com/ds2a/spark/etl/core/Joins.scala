package com.ds2a.spark.etl.core

import org.apache.spark.sql.SparkSession

object Joins {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val emp = Seq((1,"Smith",-1,"2018","10","M",3000),
      (2,"Rose",1,"2010","20","M",4000),
      (3,"Williams",1,"2010","10","M",1000),
      (4,"Jones",2,"2005","10","F",2000),
      (5,"Brown",2,"2010","40","",-1),
      (6,"Brown",2,"2010","50","",-1)
    )

    val empColumns = Seq("emp_id","name","superior_emp_id","year_joined","emp_dept_id","gender","salary")
    import spark.implicits._
    val empDF = emp.toDF(empColumns:_*)
    empDF.show(false)

    val dept =Seq(("Finance",10),
      ("Marketing",20),
      ("Sales",30),
      ("IT",40)
    )
    val deptColumns = Seq("dept_name","dept_id")
    val deptDF = dept.toDF(deptColumns:_*)
    deptDF.show(false)
    empDF.join(deptDF,empDF("emp_dept_id")=== deptDF("dept_id"),"inner").show()
    empDF.join(deptDF,empDF("emp_dept_id")=== deptDF("dept_id"),"outer").show()
    empDF.join(deptDF,empDF("emp_dept_id")=== deptDF("dept_id"),"full").show()
    empDF.join(deptDF,empDF("emp_dept_id")=== deptDF("dept_id"),"fullouter").show()
    empDF.join(deptDF,empDF("emp_dept_id")=== deptDF("dept_id"),"right").show()
    empDF.join(deptDF,empDF("emp_dept_id")=== deptDF("dept_id"),"rightouter").show()
    empDF.join(deptDF,empDF("emp_dept_id")=== deptDF("dept_id"),"left").show()
    empDF.join(deptDF,empDF("emp_dept_id")=== deptDF("dept_id"),"leftouter").show()
    empDF.join(deptDF,empDF("emp_dept_id")=== deptDF("dept_id"),"leftanti").show()
    empDF.join(deptDF,empDF("emp_dept_id")=== deptDF("dept_id"),"leftsemi").show()
    empDF.join(deptDF,empDF("emp_dept_id")=== deptDF("dept_id"),"cross").show()
    empDF.crossJoin(deptDF).show(24)
  }

}
