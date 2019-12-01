import org.apache.jena.graph
import org.apache.spark.rdd.RDD

import scala.util.Random


class OntologyMerging {
  def Merge(ontology1: RDD[graph.Triple], ontology2: RDD[graph.Triple])={
    val mergedOntology = ontology1.union(ontology2)
  }

def ResolveConflictClasses(sourceClassesWithBestTranslation: RDD[(String, String, String)], targetClassesWithoutCodes: RDD[String], listOfMatchedClasses: RDD[List[String]]) : RDD[(String, String, String)]={

  val p = new PreProcessing()

  val multilingualMatchedClasses: RDD[(String, String, String)] = listOfMatchedClasses
    .map(x => (p.stringPreProcessing(x.head).split(" ",2).last, x(1))).keyBy(_._1)
    .leftOuterJoin(sourceClassesWithBestTranslation.keyBy(_._3))
    .map{case (sourceTranslation,((sourceTranslation1,target),Some((code,source,sourceTranslation2))))=> (code,source,target)}

  val allClassesWithMultilingualInfo = this.TranslateResource(sourceClassesWithBestTranslation, multilingualMatchedClasses)

  allClassesWithMultilingualInfo
}

  def ResolveConflictRelations(similarRelations: RDD[(String, String, String, String, Double)], relationsWithTranslation: RDD[(String, String, String)]): RDD[(String, String, String)]={
    val multilingualMatchedRelations: RDD[(String, String, String)] = similarRelations.map(x=> (x._1,x._2,x._4))
    val allRelationsWithMultilingualInfo = this.TranslateResource(relationsWithTranslation, multilingualMatchedRelations)
    allRelationsWithMultilingualInfo
  }

  def TranslateResource(listOfSourceResourcesWithTranslations: RDD[(String, String, String)], multilingualMatchedResources: RDD[(String, String, String)]) : RDD[(String, String, String)]={

    val allResources = listOfSourceResourcesWithTranslations.keyBy(_._1).leftOuterJoin(multilingualMatchedResources.keyBy(_._1))
      .map{case x => if (!x._2._2.isEmpty) (x._1, x._2._1._2,x._2._2.last._3) else (x._1, x._2._1._2, x._2._1._3)}

    allResources

  }

  def GenerateURIs(allResourcesWithMultilingualInfo: RDD[(String, String, String)])={
    val rand = new Random(100000)
    val resourcesWithURIs = allResourcesWithMultilingualInfo.map(x => ((100000 + rand.nextInt(900000)).toString(), x._1, x._2, x._3))
    resourcesWithURIs.foreach(println(_))
  }

  def GenerateRundomNumbersWithoutRepitition(len: Int)={
    val rand = new Random()
    var randomNembers = new Array[Int](len)
    randomNembers(0) = 100000 + rand.nextInt(900000)
    for (i <- 1 to (randomNembers.length - 1)) {
      randomNembers(i) = randomNembers(i-1)+1
//      println(randomNembers(i))
//      println("repitition size = "+randomNembers.groupBy(identity))
//      while (randomNembers.groupBy(identity).size > 1){
//        randomNembers(i) = 100000 + rand.nextInt(900000)
//      }
    }
    randomNembers.foreach(println(_))
  }
}
