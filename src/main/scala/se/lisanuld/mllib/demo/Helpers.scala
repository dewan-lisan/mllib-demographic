package se.lisanuld.mllib.demo

import java.nio.file.{Files, Paths}

import org.apache.avro.Schema
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.StructType

object Helpers {

  def runSimpleScreening(df: DataFrame) = {
    //What's the distribution of the number of hours_per_week?
    df.selectExpr("hours_per_week").summary().show(100, false)
    //How about education status?
    df.groupBy("education")
      .count()
      .sort(desc("count"))
      .show(100, false)
  }

  def getStructTypeFromAvsc(dataSchema: String): StructType = {
    val schema: Schema = new Schema.Parser().parse(new String(Files.readAllBytes(Paths.get(dataSchema))))
    val structTypeSchema: StructType = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
    structTypeSchema
  }

  def loadData(spark: SparkSession, dataFile: String, dataSchema: String):DataFrame = {
    spark.read.format("csv")
      .option("inferSchema", "false")
      .schema(getStructTypeFromAvsc(dataSchema))
      .load(dataFile)
  }

  def getCategoricalColumns(df: DataFrame): Array[String] = df.dtypes.filter(col => col._2 == "StringType").map(_._1)

  def getCategoricalColumns(categoryColumns: String): Array[String] = categoryColumns.split(",")

  def getNumericalColumns(df: DataFrame): Array[String] = df.dtypes.filter(col => col._2 == "DoubleType").map(_._1)

  def getNumericalColumns(categoryColumns: String): Array[String] = categoryColumns.split(",")

}
