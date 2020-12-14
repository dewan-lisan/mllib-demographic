package se.lisanuld.mllib.demo

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import org.slf4j.{Logger, LoggerFactory}

/*
This is scala implementation of MLLib - binary classification
Databricks URL: https://docs.databricks.com/getting-started/spark/machine-learning.html
*/

class ExperimentProcess(spark: SparkSession) {
  val log: Logger = LoggerFactory.getLogger(classOf[ExperimentProcess])

  def process(dataFile: String, dataSchema: String, categoryColumns: String, labelColumn: String, numericColumns: String) :Unit = {
    require(!dataFile.isEmpty, "CSV file path must be provided!")
    require(!dataSchema.isEmpty, "AVSC schema file path must be provided!")

    //STEP 1: Loading the dataset
    val df: DataFrame = Helpers.loadData(spark, dataFile, dataSchema)

    //Randomly split data into training and test sets, and set seed for reproducibility.
    val Array(trainDf: Dataset[Row], testDf: Dataset[Row]) = df.randomSplit(Array(0.8, 0.2), 42)
    trainDf.cache()

    Helpers.runSimpleScreening(trainDf)

    val catColumns: Array[String] = if(!categoryColumns.isEmpty || categoryColumns != null) {
      Helpers.getCategoricalColumns(categoryColumns.toString)
    } else {
      log.info("categoryColumns hasn't been provided. All StringType columns will be taken as categoryColumns")
      Helpers.getCategoricalColumns(trainDf)
    }

    val numColumns: Array[String] = if(!numericColumns.isEmpty || numericColumns != null) {
      Helpers.getNumericalColumns(numericColumns.toString)
    } else {
      log.info("numericColumns hasn't been provided. All DoubleType columns will be taken as numericColumns")
      Helpers.getNumericalColumns(trainDf)
    }

    val labelToIndex: StringIndexer = new StringIndexer().setInputCol(labelColumn).setOutputCol("label")

    //STEP 2: Feature processing
    //Combining all features into a single feature vector. Most MLlib algorithms require a single features column as input.
    val stringIndexers: Array[StringIndexer] = catColumns.map(colName => {
                                                                            new StringIndexer()
                                                                              .setInputCol(colName)
                                                                              .setOutputCol(colName + "_index")
                                                                              //.fit(df)
                                                                          })
    val encoders: Array[OneHotEncoderEstimator] = stringIndexers.map{ stringIndex =>
                                                                          new OneHotEncoderEstimator()
                                                                            .setInputCols(Array(stringIndex.getOutputCol))
                                                                            .setOutputCols(Array(stringIndex.getOutputCol + "_enc"))
                                                                        }
    val assemblerInput: Array[String] = numColumns ++ catColumns.map(_+"_index_enc")
    val vectorAssembler: VectorAssembler = getVectorAssembledModel(assemblerInput)

    //STEP 3: Define the model
    val lr: LogisticRegression = new LogisticRegression()
      .setFeaturesCol(vectorAssembler.getOutputCol)   //defining feature column from vector assembler
      .setLabelCol(labelToIndex.getOutputCol)
      .setRegParam(1.0)

    //STEP 4: Building the pipeline based on the stages created in previous steps
    val pipeline: Pipeline = new Pipeline().setStages(stringIndexers ++ encoders ++ Array(labelToIndex) ++ Array(vectorAssembler) ++ Array(lr))
    //Defining the pipeline model
    val pipelineModel: PipelineModel = pipeline.fit(trainDf)
    //Applying the pipeline model to the test dataset
    val predDf = pipelineModel.transform(testDf)

    predDf.selectExpr("features", "label", "income", "prediction", "probability").distinct().show(100, false)

    //STEP 5: Evaluating the model
    val bcEvaluator = new BinaryClassificationEvaluator("AreaUnderROC")
    println(s"Area under ROC curve: ${bcEvaluator.evaluate(predDf)}")

    val mcEvaluator = new MulticlassClassificationEvaluator("Accuracy")
    println(s"Accuracy: ${mcEvaluator.evaluate(predDf)}")
  }

  def getVectorAssembledModel(allColumns: Array[String]) = {
    val vectorAssembler: VectorAssembler =
      new VectorAssembler()
        .setInputCols(allColumns)
        .setOutputCol("features")
        .setHandleInvalid("skip")
    vectorAssembler
  }

}
