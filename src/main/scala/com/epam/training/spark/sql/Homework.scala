package com.epam.training.spark.sql

import java.time.LocalDate

import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Homework {
  val DELIMITER = ";"
  val RAW_BUDAPEST_DATA = "data/budapest_daily_1901-2010.csv"
  val OUTPUT_DUR = "output"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("EPAM BigData training Spark SQL homework")
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.shuffle.partitions", "10")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    processData(sqlContext)

    sc.stop()

  }

  def processData(sqlContext: HiveContext): Unit = {
    /**
      * Task 1
      * Read csv data with DataSource API from provided file
      * Hint: schema is in the Constants object
      */
    val climateDataFrame: DataFrame = readCsvData(sqlContext, Homework.RAW_BUDAPEST_DATA)

    /**
      * Task 2
      * Find errors or missing values in the data
      * Hint: try to use udf for the null check
      */

    val errors: Array[Row] = findErrors(climateDataFrame)
    println(errors)

    /**
      * Task 3
      * List average temperature for a given day in every year
      */
    val averageTemeperatureDataFrame: DataFrame = averageTemperature(climateDataFrame, 1, 2)

    /**
      * Task 4
      * Predict temperature based on mean temperature for every year including 1 day before and after
      * For the given month 1 and day 2 (2nd January) include days 1st January and 3rd January in the calculation
      * Hint: if the dataframe contains a single row with a single double value you can get the double like this "df.first().getDouble(0)"
      */
    val predictedTemperature: Double = predictTemperature(climateDataFrame, 1, 2)
    println(s"Predicted temperature: $predictedTemperature")

  }

  def readCsvData(sqlContext: HiveContext, rawDataPath: String): DataFrame = {
    sqlContext
      .read
      .option("samplingRatio", "0.1")
      .option("header", "true")
      .option("delimiter", ";")
      .option("dateFormat", "yyyy-MM-dd")
      .schema(Constants.CLIMATE_TYPE)
      .csv(rawDataPath)

  }

  def findErrors(climateDataFrame: DataFrame): Array[Row] = {
    import climateDataFrame.sqlContext.implicits._
    climateDataFrame.agg(
      sum(when($"observation_date".isNotNull, 0).otherwise(1)),
      sum(when($"mean_temperature".isNotNull, 0).otherwise(1)),
      sum(when($"max_temperature".isNotNull, 0).otherwise(1)),
      sum(when($"min_temperature".isNotNull, 0).otherwise(1)),
      sum(when($"precipitation_mm".isNotNull, 0).otherwise(1)),
      sum(when($"precipitation_type".isNotNull, 0).otherwise(1)),
      sum(when($"sunshine_hours".isNotNull, 0).otherwise(1))
    ).rdd.collect()
  }


  def averageTemperature(climateDataFrame: DataFrame, monthNumber: Int, dayOfMonth: Int): DataFrame = {
    import climateDataFrame.sqlContext.implicits._
    climateDataFrame
      .select($"mean_temperature")
      .filter(month($"observation_date") === monthNumber)
      .filter(dayofmonth($"observation_date") === dayOfMonth)
  }

  def predictTemperature(climateDataFrame: DataFrame, monthNumber: Int, dayOfMonth: Int): Double = {
    climateDataFrame.createOrReplaceTempView("climate_raw")
    climateDataFrame.sqlContext.sql("SELECT avg(mean_temperature) FROM climate_raw JOIN (SELECT year(jn.observation_date) AS fldYear FROM climate_raw AS jn GROUP BY year(observation_date)) AS jnYear WHERE month(observation_date) == " + monthNumber + " AND dayofmonth(observation_date) >= " + (dayOfMonth - 1) + " AND dayofmonth(observation_date) <= " + (dayOfMonth + 1) + " AND year(observation_date) == jnYear.fldYear").first().getDouble(0);
  }
}


