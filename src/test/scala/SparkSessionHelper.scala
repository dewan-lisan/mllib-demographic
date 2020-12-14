import org.apache.spark.sql.SparkSession

trait SparkSessionHelper {
  lazy val spark = SparkSession
    .builder()
    .appName("mllib-demographic-app")
    .master("local")
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .enableHiveSupport()
    .getOrCreate()

}
