package com.kyleong.utils

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, FunSuite, Matchers}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class DateFormattingTest extends FunSuite with BeforeAndAfter with BeforeAndAfterEach with Matchers with SharedSparkContext {

  self =>

  implicit var spark: SparkSession = _

  override def beforeEach(): Unit = {}

  override def afterEach() {}

  private object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }


  before {
    spark = SparkSession.builder().master("local").getOrCreate()
  }

  after {}

  test("test regex date") {
    val util = new DateFormatting
    val str = List("20121103112058","2017-06-16 12:00:00","2016.04.01","2017年4月5日","16 June 2017","22-07-2019",
    "6. března 2015 12:00:00","27 февраля 2015","Jul 2016","12/08/2016","2 октября 2017","22 de Setembro de 2016",
    "3.1.2019","2019/07/22","2016-01-15T10:20:01Z","14. 09. 2016","2016. okt. 18.","243; datums 31 maijs 2019",
    "2017Nian 1Yue 15Ri","August 12, 2019","05/feb/2018")

    str.map(s =>println(s"input:$s\t\toutput:${util.dateFormatting(s)}"))

  }

  test("test date with spark dataframe") {
    val util = new DateFormatting
    val mappingPath = "src/main/resources/month_mapping.csv"
    val inputPath = "src/test/resources/input/raw_*.csv"
    val outputPath = "src/test/resources/output"
    val  basedf = spark.read.option("header", "true").csv(inputPath)

    val monthMappingdf = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(mappingPath)
      .withColumn("de_month", trim(lower(col("MonthName"))))
      .select("de_month","MonthNumber")
      .dropDuplicates()

    val df = basedf.withColumn("newDate", util.dateFormattingUDF(col("date")))
      .withColumn("Regex", split(col("newDate"), "-").getItem(0))
      .withColumn("in_year", split(col("newDate"), "-").getItem(1))
      .withColumn("in_month", split(col("newDate"), "-").getItem(2))
      .withColumn("in_day", split(col("newDate"), "-").getItem(3))
        .drop("date")

    val monthdf = df.join(monthMappingdf, lower(df.col("in_month"))===
      monthMappingdf.col("de_month"),"left")
      .withColumn("Year",col("in_year").cast(IntegerType))
      .withColumn("Month",col("MonthNumber").cast(IntegerType))
      .withColumn("Day",col("in_day").cast(IntegerType))


    val resultdf = monthdf.drop("newDate","in_year","in_month","in_day","de_month","MonthNumber")
        .filter(col("Year") >= 1970 && col("Year") <= 2039)
        .filter(col("Month") <= 12)
        .filter(col("Day") <= 31)

    resultdf.coalesce(1).write.mode("overwrite").option("header", true).csv(outputPath)

  }
}
