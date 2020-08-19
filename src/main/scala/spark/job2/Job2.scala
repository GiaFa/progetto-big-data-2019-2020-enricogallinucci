package spark.job2

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

object Job2{
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("Progetto BigData").getOrCreate()
    val RANGE_SCORE = (2,4,5)
    val RANGE_SCORE_B: Broadcast[(Int,Int,Int)] = sparkSession.sparkContext.broadcast(RANGE_SCORE)
    TopBeers.executeJob(sparkSession,RANGE_SCORE_B)
  }
}
