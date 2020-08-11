package spark

object Breweries {
  def extract(row:String):Breweries = {
    def getInt(str:String) : Int = {
      if (str.isEmpty)
        0
      else
        str.toInt
    }
    val columns = row.split(",").map(_.replaceAll("\"",""))
    val id = getInt(columns(0))
    Breweries(id,columns(1))
  }
}

case class Breweries(id: Int, name: String)
