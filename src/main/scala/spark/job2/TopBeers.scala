package spark.job2

import org.apache.hadoop.fs.Path
import spark.SessionSpark

object TopBeers extends SessionSpark{
  def executeJob(nBirrerie: Int = 20,nClass: Int = 5, minAvgScore: Int = 2): Unit = ???

  def verifyExistFile(): Unit ={
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    if( fs.exists(new Path("/path/on/hdfs/to/SUCCESS.txt"))){
      fs.delete(new Path("/path/on/hdfs/to/SUCCESS.txt"),false)
    }
  }

}
