package com.bosch.test

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
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


    val spark =SparkSession.builder().appName("IsbnEncoder").master("local").getOrCreate()

    import spark.implicits._


    val newDF=df.filter(df("isbn") rlike """ISBN: \d{3}-\d{10}""")

    if (newDF.count()==0){
      return df
    }

    val strial=newDF.collect()

    val d=strial.map(x=>List((x.get(0).toString,x.get(1).toString.toInt,"ISBN-EAN: "+x.get(2).toString.substring(6,9)),
      (x.get(0).toString,x.get(1).toString.toInt,"ISBN-GROUP: "+x.get(2).toString.substring(10,12)),
      (x.get(0).toString,x.get(1).toString.toInt,"ISBN-PUBLISHER: "+x.get(2).toString.substring(12,16)),
      (x.get(0).toString,x.get(1).toString.toInt,"ISBN-TITLE: "+x.get(2).toString.substring(16,19))

      ))

    val f=d.flatten

    val tempDF=f.toSeq.toDF()

    return df.union(tempDF)
  }

}
