package spark.job2

import java.util.regex.Pattern

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

object Job2{
  private val N_BIRRERIE = 20
  private val N_RECENSIONI = 50
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("Progetto BigData").getOrCreate()
    val RANGE_SCORE = (2,4,5)
    val RANGE_SCORE_B: Broadcast[(Int,Int,Int)] = sparkSession.sparkContext.broadcast(RANGE_SCORE)
    val params = setGlobalVariable(args)
    TopBeers.executeJob(sparkSession,RANGE_SCORE_B,params._1, params._2)
  }

  private def setGlobalVariable(args: Array[String]): (Int, Int) = {

    var res = (N_BIRRERIE,N_RECENSIONI)
    if (args.length > 0 && Pattern.matches("([0-9]*)", args(0))) {
      res = (Integer.parseInt(args(0)),res._2)
    }
    if (args.length > 1 && Pattern.matches("([0-9]*)", args(1))) {
      res = (res._1,Integer.parseInt(args(1)))
    }
    res
  }
}
