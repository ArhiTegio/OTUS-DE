import org.apache.spark.sql.execution.streaming.StreamMetadata.format
import org.json4s._
import org.json4s.jackson.JsonMethods._



object JsonReader extends App  {
  case class User(id:Int, country:String, points:Int, price:Float, title:String, variety:String, winery:String)
  var path_ = "C:\\Git\\Scala\\Задание 3\\2.Java\\target\\winemag-data-130k-v2.json"
  if (args.length > 0) {
    println("---")
    println(args(0))
    println("---")
    path_ = args(0)
  }
  val jsons = scala.io.Source.fromFile(path_).mkString
    .split('}')

  var users = List[User]()
  for(json <- jsons) {
    var j = json
    if(j.length > 10) {
      if (!json.contains("id\":")) {
        j = j + ", \"id\":-1"
      }
      if (!json.contains("points\":")) {
        j = j + ", \"points\":-1"
      }
      if (!json.contains("price\"")) {
        j = j + ", \"price\":-1"
      }
      if (!json.contains("country\":")) {
        j = j + ", \"country\":\"\""
      }
      if (!json.contains("title\":")) {
        j = j + ", \"title\":\"\""
      }
      if (!json.contains("variety\":")) {
        j = j + ", \"variety\":\"\""
      }
      if (!json.contains("winery\":")) {
        j = j + ", \"winery\":\"\""
      }

      val decodedUser = parse(j + "}").extract[User]
      users =  decodedUser :: users
    }
  }

  for(line <- users){
    println(line)
  }
}
