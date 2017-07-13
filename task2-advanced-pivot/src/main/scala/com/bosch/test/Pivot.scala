package com.bosch.test

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by saschavetter on 06/07/16.
  */

object Pivot {
  val TypeMeasurement = "measurement"
  val TypeTest = "test"

  val TestPassed = "passed"
  val TestFailed = "failed"

  val AggFirst = "first"
  val AggLast = "last"

  implicit def pivot(df: DataFrame) = new PivotImplicit(df)
}

class PivotImplicit(df: DataFrame) extends Serializable {

  /**
    * Pivots machine data
    *
    * @return machine data pivoted
    */
  def getTests(): DataFrame = {
    df
  }

}