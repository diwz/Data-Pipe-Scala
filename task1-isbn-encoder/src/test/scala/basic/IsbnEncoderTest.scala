package basic

import com.bosch.test.IsbnEncoder._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.{After, Before, Test}

/**
  * Created by sascha on 6/14/16.
  */
@Test
class IsbnEncoderTest {

  private var spark: SparkSession = null

  /**
    * Create Spark context before tests
    */
  @Before
  def setUp(): Unit = {
    spark = {
      SparkSession.builder().appName("IsbnEncoderTest").master("local").getOrCreate()
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
  def TestValidIsbn(): Unit = {
    val r1 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, "ISBN: 978-1449358624")

    val records = Seq(r1)
    val df = spark.createDataFrame(records)

    val df_r = df.explodeIsbn()

    assert(5 == df_r.count())
    assert(5 == df_r.filter(col("name") === "Learning Spark: Lightning-Fast Big Data Analysis").count())
    assert(5 == df_r.filter(col("year") === 2015).count())

    assert(1 == df_r.filter(col("isbn") === "ISBN: 978-1449358624").count())
    assert(1 == df_r.filter(col("isbn") === "ISBN-EAN: 978").count())
    assert(1 == df_r.filter(col("isbn") === "ISBN-GROUP: 14").count())
    assert(1 == df_r.filter(col("isbn") === "ISBN-PUBLISHER: 4935").count())
    assert(1 == df_r.filter(col("isbn") === "ISBN-TITLE: 862").count())
  }

  @Test
  def TestInvalidIsbn(): Unit = {
    val r1 = Isbn("My book", 2014, "INVALID-ISBN")

    val records = Seq(r1)
    val df = spark.createDataFrame(records)

    val df_r = df.explodeIsbn()

    assert(1 == df_r.count())
    assert(df_r.first().get(2) == "INVALID-ISBN")
  }

  @Test
  def TestEmptyIsbn(): Unit = {
    val r1 = Isbn("My book", 2014, "")

    val records = Seq(r1)
    val df = spark.createDataFrame(records)

    val df_r = df.explodeIsbn()

    assert(1 == df_r.count())
    assert(df_r.first().get(2) == "")
  }

  @Test
  def TestMixed(): Unit = {
    val r1 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, "ISBN: 978-1449358624")
    val r2 = Isbn("Scala Tutorial", 2016, "No ISBN so far")

    val records = Seq(r1, r2)
    val df = spark.createDataFrame(records)

    val df_r = df.explodeIsbn()

    assert(6 == df_r.count())
    assert(5 == df_r.filter(col("name") === "Learning Spark: Lightning-Fast Big Data Analysis").count())
    assert(1 == df_r.filter(col("name") === "Scala Tutorial").count())
  }

  case class Isbn(name: String, year: Int, isbn: String)
}
