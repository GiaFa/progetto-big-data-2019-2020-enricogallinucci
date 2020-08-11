package spark

package spark

import java.util.regex.Pattern

object Beers {
  def extract(row:String):Beers = {
    def getInt(str:String) : Int = {
      if (str.isEmpty)
        0
      else
        str.toInt
    }
    val columns = row.split(",").map(_.replaceAll("\"",""))
    val id = getInt(columns(0))
    var i = 2
    var name = columns(1)
    while(!Pattern.matches("([0-9]*)", columns(i))){
      name = name.concat(columns(i))
      i = i+1
    }
    val brewery = getInt(columns(i))
    Beers(id,name,brewery)
  }
}

case class Beers(id: Int, name: String, brewery_id: Int)
