import org.apache.spark.sql.execution.command.ResetCommand.p
import org.apache.spark.sql.execution.streaming.StreamMetadata.format
import org.json4s._
import org.json4s.jackson.JsonMethods._



object JsonReader extends App  {
  case class User(id:Int, country:String, points:Int, price:Float, title:String, variety:String, winery:String)
  val path_ = if (args.length > 0) {
    println("---")
    println(args(0))
    println("---")
    args(0)
  } else "C:\\Git\\Scala\\winemag-data-130k-v2.json"

  val jsons = scala.io.Source.fromFile(path_).mkString.split('\n')
  val users = jsons.map(j => {
    val p = parse(j)
    val txt = p.values.toString
    if (txt.contains("id ->")
      && txt.contains("points ->")
      && txt.contains("price ->")
      && txt.contains("country ->")
      && txt.contains("title ->")
      && txt.contains("variety ->")
      && txt.contains("winery ->")) {
      p.extract[User]
    }
  })

  for(line <- users){
    println(line)
  }
}
