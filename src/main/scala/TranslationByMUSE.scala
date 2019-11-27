import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class TranslationByMUSE(sparkSession: SparkSession) extends Serializable {
  val dicDe_En = sparkSession.sparkContext.textFile("/home/shimaa/OMCM/src/main/resources/Dictionary/De_En").map(x => (x.split(" ").head, x.split(" ").last)).keyBy(_._1).collect().toMap
  val dicBroadcasting: Broadcast[Map[String, (String, String)]] = sparkSession.sparkContext.broadcast(dicDe_En)

  val pre = new PreProcessing()

  def TranslateGermanToEnglish(sourceClassesWithCodes: RDD[(String, String)]) = {
    println("Dictionary:")
    val trans: RDD[(String, String, String)] = sourceClassesWithCodes.map(x => (x._1, x._2, this.GetTranslation(x._2))) //    val trans: RDD[(String, String, String)] = sourceClassesWithCodes.map { case y => if (y._2 != null) (y._1, y._2, this.GetTranslation(y._2)) else (y._1, y._2, " ") } //    println(trans.count())
    trans.foreach(println(_))
  }

  def GetTranslation(phrase: String): String = {
    var translatedPhrase: String = ""
    var a: Array[String] = phrase.toString.replaceAll("-"," ").toLowerCase.split(" ")
    for (i <- 0 to a.length - 1) yield {
      //        println(i, a(i))
      if (dicBroadcasting.value.contains(a(i))) translatedPhrase += dicBroadcasting.value(a(i))._2.concat(" ") else translatedPhrase += a(i).concat(" ")
    }
//    pre.ToCamelForClass(pre.splitCamelCase(translatedPhrase).toLowerCase.replaceAll("\\s+", " "))
    pre.splitCamelCase(translatedPhrase).toLowerCase.replaceAll("\\s+", " ")
  }

}
