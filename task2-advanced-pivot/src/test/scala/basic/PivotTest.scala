package basic

import com.bosch.test.Pivot._
import com.bosch.test.Pivot
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.{After, Before, Test}

/**
  * Created by sascha on 6/14/16.
  */
@Test
class PivotTest {

  private var spark: SparkSession = null

  /**
    * Create Spark context before tests
    */
  @Before
  def setUp(): Unit = {
    spark = {
      SparkSession.builder().appName("PivotTest").master("local").getOrCreate()
    }
  }

  /**
    * Stop Spark context after tests
    */
  @After
  def tearDown(): Unit = {
    spark.stop()
    spark = null
  }

  @Test
  def TestWithoutRecursions(): Unit = {
    val r1 = Result("x01", "ST 10", "Pressure", Pivot.TypeMeasurement, 1499110000L, 12.2d, "NA")
    val r2 = Result("x01", "ST 30", "Final test", Pivot.TypeTest, 1499112100L, Double.NaN, Pivot.TestPassed)

    val records = Seq(r1, r2)
    val df = spark.createDataFrame(records)

    val df_r = df.getTests()

    /*
    === expected result
    | Part | Location   | Test       | TestResult  | Aggregation  | Features           |
    | x01  | ST 30      | Final test | passed      | first        | [(Pressure, 12.2)] |
    | x01  | ST 30      | Final test | passed      | last         | [(Pressure, 12.2)] |
     */

    // has two entries for first and last aggregation
    assert(2 == df_r.count())

    // has two entries for test "Final test"
    assert(2 == df_r.filter(col("test") === "Final test").count())

    // test result is correct for first and last aggregation
    assert(2 == df_r.filter(col("test") === "Final test" && col("testResult") === Pivot.TestPassed).count())

    // size of list or vector is equal one
    assert(2 == df_r.filter(size(col("features")) === 1).count())
  }

  @Test
  def TestPartHasRecursions(): Unit = {
    val r1 = Result("x01", "ST 10", "Pressure", Pivot.TypeMeasurement, 1499110000L, 12.2d, "NA")
    val r2 = Result("x01", "ST 30", "Final test", Pivot.TypeTest, 1499112100L, Double.NaN, Pivot.TestPassed)
    val r3 = Result("x01", "ST 30", "Visual test", Pivot.TypeTest, 1499112100L, Double.NaN, Pivot.TestFailed)
    val r4 = Result("x01", "ST 10", "Pressure", Pivot.TypeMeasurement, 1499112810L, 12.3d, "NA")
    val r5 = Result("x01", "ST 10", "Pressure", Pivot.TypeMeasurement, 1499112900L, 12.4d, "NA")
    val r6 = Result("x01", "ST 30", "Final test", Pivot.TypeTest, 1499113200L, Double.NaN, Pivot.TestPassed)
    val r7 = Result("x01", "ST 30", "Visual test", Pivot.TypeTest, 1499113300L, Double.NaN, Pivot.TestPassed)

    val records = Seq(r1, r2, r3, r4, r5, r6, r7)
    val df = spark.createDataFrame(records)

    val df_r = df.getTests()

    /*
    === expected result
    | Part | Location   | Test        | TestResult  | Aggregation  | Features           |
    | x01  | ST 30      | Final test  | passed      | first        | [(Pressure, 12.2)] |
    | x01  | ST 30      | Final test  | passed      | last         | [(Pressure, 12.4)] |
    | x01  | ST 30      | Visual test | failed      | first        | [(Pressure, 12.2)] |
    | x01  | ST 30      | Visual test | passed      | last         | [(Pressure, 12.4)] |
     */

    // has two entries for first and last aggregation
    assert(4 == df_r.count())

    // has two entries for each test
    assert(2 == df_r.filter(col("test") === "Final test").count())
    assert(2 == df_r.filter(col("test") === "Visual test").count())

    // test result is correct for first and last aggregation
    assert(2 == df_r.filter(col("test") === "Final test" && col("testResult") === Pivot.TestPassed).count())

    // test result is correct for first and last aggregation
    assert(1 == df_r.filter(col("test") === "Visual test" && col("aggregation") === Pivot.AggFirst && col("testResult") === Pivot.TestFailed).count())
    assert(1 == df_r.filter(col("test") === "Visual test" && col("aggregation") === Pivot.AggLast && col("testResult") === Pivot.TestPassed).count())

    // size of list or vector is equal one
    assert(4 == df_r.filter(size(col("features")) === 1).count())

    // ensure that feature values are correct for "Visual test"
    assert(1 == df_r.filter(col("test") === "Visual test" && col("aggregation") === Pivot.AggFirst && col("features").getItem(0).getItem("value") === 12.2).count())
    assert(1 == df_r.filter(col("test") === "Visual test" && col("aggregation") === Pivot.AggLast && col("features").getItem(0).getItem("value") === 12.4).count())
  }

  @Test
  def TestManyParts(): Unit = {
    val r1 = Result("x01", "ST 10", "Pressure", Pivot.TypeMeasurement, 1499110000L, 12.2d, "NA")
    val r2 = Result("x01", "ST 30", "Final test", Pivot.TypeTest, 1499112100L, Double.NaN, Pivot.TestPassed)
    val r3 = Result("x02", "ST 10", "Pressure", Pivot.TypeMeasurement, 1499112810L, 12.3d, "NA")
    val r4 = Result("x02", "ST 10", "Flow", Pivot.TypeMeasurement, 1499112900L, 0.2d, "NA")
    val r5 = Result("x02", "ST 30", "Final test", Pivot.TypeTest, 1499113200L, Double.NaN, Pivot.TestPassed)

    val records = Seq(r1, r2, r3, r4, r5)
    val df = spark.createDataFrame(records)

    val df_r = df.getTests()

    /*
    === expected result
    | Part | Location   | Test        | TestResult  | Aggregation  | Features           |
    | x01  | ST 30      | Final test  | passed      | first        | [(Pressure, 12.2)] |
    | x01  | ST 30      | Final test  | passed      | last         | [(Pressure, 12.2)] |
    | x02  | ST 30      | Final test  | passed      | first        | [(Pressure, 12.3), (Flow, 0.2)] |
    | x02  | ST 30      | Final test  | passed      | last         | [(Pressure, 12.3), (Flow, 0.2)] |
     */

    // has two entries for first and last aggregation
    assert(4 == df_r.count())

    // has four tests
    assert(4 == df_r.filter(col("test") === "Final test").count())

    // all tests passed
    assert(4 == df_r.filter(col("test") === "Final test" && col("testResult") === Pivot.TestPassed).count())

    // size of list or vector is equal two for part x02
    assert(2 == df_r.filter(col("part") === "x02" && size(col("features")) === 2).count())
  }

  case class Result(part: String, location: String, name: String, mType: String, unixTimestamp: Long, value: Double, testResult: String)
}
