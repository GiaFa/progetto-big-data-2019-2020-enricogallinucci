package spark

object Reviews {
  def extract(row:String):Reviews = {
    def getDouble(str:String) : Double = {
      if (str.isEmpty)
        0
      else
        str.toDouble
    }

    val columns = row.split(",").map(_.replaceAll("\"",""))
    val id = columns(0).toInt
    val overall = getDouble(columns.last)
    Reviews(id,overall)
  }
}

case class Reviews(beer_id: Int, overall: Double)
