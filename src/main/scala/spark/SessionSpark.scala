package spark

import org.apache.spark.sql.SparkSession

abstract class SessionSpark extends Serializable {
  implicit val sparkSession: SparkSession = SparkSession.builder().appName("Progetto BigData").getOrCreate()
}
