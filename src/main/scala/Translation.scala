import org.apache.spark.rdd.RDD
import zhmyh.yandex.api.translate.{Language, Translate}

object Translation extends Serializable {
  val tr = new Translate("trnsl.1.1.20190411T144635Z.594931b10ad4385a.829ff83b6878ab076f769297aea1e725f168f30a")

  /**
    * Translate a sentence from any language to English.*/
  def yandexTranslation(sentence:String, langaugeTag:String):String={
    var result = ""
      if (langaugeTag == "de")
        result= tr.translate(sentence, Language.GERMAN, Language.ENGLISH).get
      else if (langaugeTag == "ar")
        result= tr.translate(sentence, Language.ARABIC, Language.ENGLISH).get
      else if (langaugeTag == "fr")
        result= tr.translate(sentence, Language.FRENCH, Language.ENGLISH).get
//      else if (langaugeTag == "zh")
//        result= tr.translate(sentence, Language.Chinese, Language.ENGLISH).get
    result
  }

  /**
    * Detect a language of a sentence.*/
  def languageDetection(sentence:String)={
    val lan = tr.detect(sentence)
    println("The detected language is: "+lan.get.toString)
  }
//  def languageDetection()={
//    val lan = tr.detect("每篇论文审稿意见数")
//    println("The detected language is: "+lan.get.toString)
//  }

  /**
    * Translate classes and relations to English.*/
  def translateToEnglish(Oclasses: RDD[String], Orelations: RDD[String], langaugeTag:String)={
    val classesWithTranslation = Oclasses.map(x => (x,this.yandexTranslation(x, langaugeTag)))
    println("=====================")
    println("Translated classes:")
    println("=====================")
    classesWithTranslation.foreach(println(_))
//    classesWithTranslation.coalesce(1, shuffle = true).saveAsTextFile("src/main/resources/Output/Translations/classesWithTranslation")
    classesWithTranslation.map{case(a, b) =>
      var line = a.toString + "," + b.toString
      line
    }.coalesce(1, shuffle = true).saveAsTextFile("src/main/resources/Output/Translations/classesWithTranslation")

    val RelationsWithTranslation: RDD[(String, String)] = Orelations.map(x => (x,this.yandexTranslation(x, langaugeTag)))
    println("=====================")
    println("Translated relations:")
    println("=====================")
    RelationsWithTranslation.foreach(println(_))
//    RelationsWithTranslation.coalesce(1, shuffle = true).saveAsTextFile("src/main/resources/Output/Translations/RelationsWithTranslation")
    RelationsWithTranslation.map{case(a, b) =>
      var line = a.toString + "," + b.toString
      line
    }.coalesce(1, shuffle = true).saveAsTextFile("src/main/resources/Output/Translations/RelationsWithTranslation")
  }

}
