package spark.job1

import java.util.regex.Pattern

import org.apache.spark.sql.SparkSession

/** Top N birrerie(N passabile come parametro, default 20) con almeno N birre diverse(N secondo parametro, default 5)
* con le medie di voti piu alta.(N recensioni minime per ogni birra, terzo parametro, 50 default).
*/
object Job1{
  private val N_BIRRERIE = 20
  private val N_RECENSIONI = 50
  private val N_BIRRE = 5
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("Progetto BigData").getOrCreate()
    val param = setGlobalVariable(args)
    TopBreweries.executeJob(sparkSession,param._1,param._3,param._2)
  }

  private def setGlobalVariable(args: Array[String]): (Int, Int,Int) = {

    var res = (N_BIRRERIE,N_RECENSIONI,N_BIRRE)
    if (args.length > 0 && Pattern.matches("([0-9]*)", args(0))) {
      res = (Integer.parseInt(args(0)),res._2,res._3)
    }
    if (args.length > 1 && Pattern.matches("([0-9]*)", args(1))) {
      res = (res._1,Integer.parseInt(args(1)),res._3)
    }
    if (args.length > 2 && Pattern.matches("([0-9]*)", args(2))) {
      res = (res._1,res._2,Integer.parseInt(args(2)))
    }
    res
  }
}
