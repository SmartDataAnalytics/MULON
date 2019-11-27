import org.apache.jena.graph
import org.apache.spark.rdd.RDD

class OntologyMerging {
  def Merge(ontology1: RDD[graph.Triple], ontology2: RDD[graph.Triple])={
    val mergedOntology = ontology1.union(ontology2)
  }
  def ResolveConflictClasses(sourceClassesWithBestTranslation: RDD[(String, String, String)], targetClassesWithoutCodes: RDD[String], listOfMatchedClasses: RDD[List[String]])={
    val p = new PreProcessing()
    val multilingualMatchedClasses = listOfMatchedClasses.map(x => (p.stringPreProcessing(x.head).split(" ",2).last, x(1))).keyBy(_._1).leftOuterJoin(sourceClassesWithBestTranslation.keyBy(_._3))
    println("Multilingual Matched Classes")
    multilingualMatchedClasses.foreach(println(_))

  }
}