package se.lisanuld.mllib.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AppsRunner extends App {
  lazy val spark = SparkSession
    .builder()
    .config(new SparkConf().setAppName("mllib-demo"))
    .master("local")
    .getOrCreate()

  new ExperimentProcess(spark).process(spark.sparkContext.getConf.get("spark.lisanuld.datafile", "src/test/resources/adult.data"),
                                        spark.sparkContext.getConf.get("spark.lisanuld.dataschema", "src/test/resources/adult.data.schema.avsc"),
                                        spark.sparkContext.getConf.get("spark.lisanuld.categoryColumns", "workclass,education,marital_status,occupation,relationship,race,sex"),
                                        spark.sparkContext.getConf.get("spark.lisanuld.labelColumn", "income"),
                                        spark.sparkContext.getConf.get("spark.lisanuld.numericColumns", "age,fnlwgt,education_num,capital_gain,capital_loss,hours_per_week")
                                        )

  spark.close()
}
