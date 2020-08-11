package spark.job1

import spark.SessionSpark

object Job1{
  def main(args: Array[String]): Unit = {
    TopBreweries.executeJob()
  }
}
