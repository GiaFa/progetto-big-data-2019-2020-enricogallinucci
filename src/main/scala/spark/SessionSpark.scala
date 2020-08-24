package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

abstract class SessionSpark extends Serializable {

  def readFile(sparkSession: SparkSession): (RDD[String], RDD[String], RDD[String]) ={
    val beers = sparkSession.sparkContext.textFile("giovannim/dataset/input/datasetprogetto/beers.csv")
    val reviews = sparkSession.sparkContext.textFile("giovannim/dataset/input/datasetprogetto/reviews.csv")
    val breweries = sparkSession.sparkContext.textFile("giovannim/dataset/input/datasetprogetto/breweries.csv")
    (beers,reviews,breweries)
  }
}
