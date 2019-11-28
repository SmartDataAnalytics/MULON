import org.apache.jena.graph
import org.apache.spark.rdd.RDD

class OntologyMerging {
  def Merge(ontology1: RDD[graph.Triple], ontology2: RDD[graph.Triple])={
    val mergedOntology = ontology1.union(ontology2)
  }
  def ResolveConflictClasses(sourceClassesWithBestTranslation: RDD[(String, String, String)], targetClassesWithoutCodes: RDD[String], listOfMatchedClasses: RDD[List[String]])={
    val p = new PreProcessing()
    val multilingualMatchedClasses: RDD[(String, String, String)] = listOfMatchedClasses
      .map(x => (p.stringPreProcessing(x.head).split(" ",2).last, x(1))).keyBy(_._1)
      .leftOuterJoin(sourceClassesWithBestTranslation.keyBy(_._3))
      .map{case (sourceTranslation,((sourceTranslation1,target),Some((code,source,sourceTranslation2))))=> (code,source,target)}
    println("Multilingual Matched Classes")
    multilingualMatchedClasses.foreach(println(_))
    this.TranslateOntology(sourceClassesWithBestTranslation, multilingualMatchedClasses)
  }
  def TranslateOntology(sourceClassesWithBestTranslation: RDD[(String, String, String)], multilingualMatchedClasses: RDD[(String, String, String)])={
    val allClasses = sourceClassesWithBestTranslation.keyBy(_._1).leftOuterJoin(multilingualMatchedClasses.keyBy(_._1))
      .map{case x => if (!x._2._2.isEmpty) (x._1, x._2._1._2,x._2._2.last._3) else (x._1, x._2._1._2, x._2._1._3)}
//      .map{case (code,((code1, source,translation),(x,y,z))) => if(!y.isEmpty) (code,source,y.)}
    allClasses.foreach(println(_))

  }
}
