import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import se.lisanuld.mllib.demo.{Helpers}


class HelpersTest extends FunSuite with SparkSessionHelper with BeforeAndAfterAll {
  val dataPath = "src/test/resources/adult.data"
  val dataSchema = "src/test/resources/adult.data.schema.avsc"
  var df: DataFrame = _

  override def beforeAll(): Unit = {
    df = Helpers.loadData(spark, dataPath, dataSchema)
  }

  override def afterAll(): Unit = {spark.close()}

//  test("Experiment process test"){
//    new ExperimentProcess(spark).process(dataPath)
//  }

  test("loadData test including schema"){
//    val df = new ExperimentProcess(spark).loadData(dataPath, dataSchema)
    df.printSchema()
    assert(df.count() === 32561)
    df.dtypes.foreach(println)
    assert(df.dtypes(0)._2 === "DoubleType")

    val schemaMap: Map[String, String] = df.dtypes.map(x => (x._1, x._2)).toMap
    assert(schemaMap("age") === "DoubleType")
    assert(schemaMap("income") === "StringType")
  }

  test("getCategoricalColumns test") {
    val stringColumns: Array[String] = Helpers.getCategoricalColumns(df)
    stringColumns.foreach(println)
    assert(stringColumns.contains("education"))
    assert(!stringColumns.contains("age"))
  }
}
