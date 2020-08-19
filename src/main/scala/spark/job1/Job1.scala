package spark.job1

import org.apache.spark.sql.SparkSession
import spark.SessionSpark

object Job1{
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("Progetto BigData").getOrCreate()
    TopBreweries.executeJob(sparkSession)
  }
}
