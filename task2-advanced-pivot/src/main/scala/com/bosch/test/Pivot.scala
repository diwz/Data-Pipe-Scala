package com.bosch.test

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
import org.apache.log4j.Logger
import org.apache.log4j.Level

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
    val spark =SparkSession.builder().appName("parts").master("local").getOrCreate()
Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    import spark.implicits._
    df.createOrReplaceTempView("trial")
    val First_Measure=spark.sql("""select part,'first' as Aggregation,unixTimestamp as ut,concat('(',name,',',value,')') as Features from trial""").filter($"mType".like("measurement")).groupBy($"part",$"Aggregation").agg(expr("first(Features) as Features"))

    val Last_Measure=spark.sql("""select part,'last' as Aggregation,unixTimestamp as ut,concat('(',name,',',value,')') as Features from trial""").filter($"mType".like("measurement")).groupBy($"part",$"Aggregation").agg(expr("last(Features) as Features"))

    val MeasureWithAggregation=First_Measure.union(Last_Measure)

    val y=MeasureWithAggregation.as("f1").join(MeasureWithAggregation.as("f2"),$"f1.part"===$"f2.part" && $"f1.Features".=!=($"f2.Features")  && $"f1.Aggregation".=!=($"f2.Aggregation") ).filter($"f1.Features".substr(0,2)=!=$"f2.Features".substr(0,2)).withColumn("Features1",expr("concat('[',f1.Features,',',f2.Features,']')")).select($"f1.part",$"f1.Aggregation",$"Features1")

    val GroupedMeasure=y.select($"part",$"Aggregation",$"Features1")

    val d=MeasureWithAggregation.select("part").intersect(GroupedMeasure.select("part"))

    val UniqueMeasure=MeasureWithAggregation.join(d,Seq("part"),"leftanti").withColumn("Features",expr("concat('[',Features,']')"))

    val FinalMeasures=UniqueMeasure.union(GroupedMeasure)

    val FirstTest=spark.sql("""select part,location,'first' as Aggregation,name,testResult from trial""").filter($"mType".like("test")).groupBy($"part",$"location",$"Aggregation",$"name").agg(first($"testResult").as("testResult"),expr("first(name) as Name1"))

    val LastTest=spark.sql("""select part,location,'last' as Aggregation,name,testResult from trial""").filter($"mType".like("test")).groupBy($"part",$"location",$"Aggregation",$"name").agg(last($"testResult").as("testResult"),expr("last(name) as Name2"))

    val AllTests=FirstTest.select($"part",$"location",$"Aggregation",$"Name1",$"testResult").union(LastTest.select($"part",$"location",$"Aggregation",$"Name2",$"testResult"))

    return AllTests.as("at").join(FinalMeasures.as("fm"),$"at.part"===$"fm.part" && $"at.Aggregation"===$"fm.aggregation").select($"at.part".as("Part"),$"at.location".as("Location"),$"at.Name1".as("Test"),$"testResult",$"at.Aggregation",$"fm.Features")


  }

}