import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class TestClassSimilarity {
  def GetBestTranslationForClass(listOfTranslations: List[String], targetClassesBroadcasting: Broadcast[Map[String, Long]]): List[Any]={
    val sp = SparkSession.builder
      //      .master("spark://172.18.160.16:3090")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    var bestTranslation: List[Any] = List(" "," ", 0.0)
    //    var bestTranslation: List[String] = List(" ")

    val gS = new GetSimilarity()
    var targetClasses: RDD[String] = sp.sparkContext.parallelize(targetClassesBroadcasting.value.map(_._1).toList)//.cache()
    var translations = sp.sparkContext.parallelize(listOfTranslations)
    var crossRDD: RDD[(String, String)] = translations.cartesian(targetClasses)
    //    println("The cross RDD is:")
    //    crossRDD.foreach(println(_))
    //    var sim: RDD[(String, String, Double)] = crossRDD.map(x=>(x._1,x._2,gS.getJaccardStringSimilarity(x._1,x._2))).filter(y=>y._3>=0.3)
    //    var sim: RDD[(String, String, Double)] = crossRDD.map(x=>(x._1,x._2,semSim.getPathSimilarity(x._1.split(" ").last,x._2.split(" ").last))).filter(y=>y._3>=0.6)
    var sim: RDD[(String, String, Double)] = crossRDD.map(x=>(x._1.toLowerCase,x._2.toLowerCase,gS.getSimilarity(x._1,x._2))).filter(y=>y._3>0.9)
    //    sim.foreach(println(_))
    var matchedTerms: RDD[(String, String, Double)] = sim//.map(x=>(x._2,x._3))
    if (!matchedTerms.isEmpty()){
      bestTranslation = matchedTerms.collect().toList//.map(x=>(x._1))
    }
    else bestTranslation = listOfTranslations

    bestTranslation
  }

  /**
    * Get the similarity between two classes in two different ontologies.*/
  def GetClassSimilarity(O1Translated: RDD[(String, String)], O2: RDD[String]): RDD[(String, String, String, Double)]={
    val gS = new GetSimilarity()
    val t = O1Translated.cartesian(O2)
    val tt: RDD[(String, String, String, Double)] = t.map(x => (x._1._1, x._1._2, x._2,gS.getSimilarity(x._1._2.toLowerCase,x._2.toLowerCase))).filter(y=>y._4>0.9)
//    tt.foreach(println(_))
    tt
  }

}
