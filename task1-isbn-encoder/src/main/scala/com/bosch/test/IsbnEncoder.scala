package com.bosch.test

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by saschavetter on 06/07/16.
  */

object IsbnEncoder {
  implicit def dmcEncoder(df: DataFrame) = new IsbnEncoderImplicit(df)
}

class IsbnEncoderImplicit(df: DataFrame) extends Serializable {

  /**
    * Creates a new row for each element of the ISBN code
    *
    * @return a data frame with new rows for each element of the ISBN code
    */
  def explodeIsbn(): DataFrame = {
    df
  }
}
