import org.apache.jena.graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


class OntologyMerging(sparkSession: SparkSession) {
  val gCreate = new GraphCreating()

  def Merge(sourceClassesWithBestTranslation: RDD[(String, String, String)], targetClassesWithoutCodes: RDD[String], listOfMatchedClasses: RDD[List[String]], similarRelations: RDD[(String, String, String, String, Double)], relationsWithTranslation: RDD[(String, String, String)], sOntology: RDD[(String, String, String)]) = {

    val allClassesWithMultilingualInfo = this.ResolveConflictClasses(sourceClassesWithBestTranslation, targetClassesWithoutCodes, listOfMatchedClasses)

    val RelationsWithMultilingualInfo = this.ResolveConflictRelations(similarRelations, relationsWithTranslation) //      println("All resources for source ontology with multilingual info after resolving conflicts:")
    //    allClassesWithMultilingualInfo.union(RelationsWithMultilingualInfo).foreach(println(_))
    //    om.GenerateURIsForResources(allClassesWithMultilingualInfo)

    val sourceClassesWithURIs: RDD[(String, String, String)] = this.GenerateURIsForResources(allClassesWithMultilingualInfo.map(x => (x._2, x._3)), 'C')
    //    println("Source classes with URIs"+sourceClassesWithURIs.count())
    //    sourceClassesWithURIs.foreach(println(_))

    val sourceRelationsWithURIs = this.GenerateURIsForResources(RelationsWithMultilingualInfo.map(x => (x._2, x._3)), 'P') //    println("Source relations with URIs"+sourceRelationsWithURIs.count())
    //    sourceRelationsWithURIs.foreach(println(_))

    val translatedOntology: RDD[graph.Triple] = this.TranslateSourceOntology(sourceClassesWithURIs.union(sourceRelationsWithURIs), sOntology) //    println("Translate ontology with number of triples")
    //    translatedOntology.foreach(println(_))

    val englishLabels = gCreate.CreateMultilingualEnglishLabels(sourceClassesWithURIs.union(sourceRelationsWithURIs)) //    println("English labels"+englishLabels.count())
    //    englishLabels.foreach(println(_))

    val germanLabels = gCreate.CreateMultilingualGermanLabels(sourceClassesWithURIs.union(sourceRelationsWithURIs)) //    println("German labels"+germanLabels.count())
    //    germanLabels.foreach(println(_))

    val O1 = translatedOntology.union(englishLabels).union(germanLabels)
//    println("Multilingual O1")
//    O1.take(20).foreach(println(_))
//    O1.coalesce(1, shuffle = true).saveAsNTriplesFile("src/main/resources/Output/O1")
    val ontStat = new OntologyStatistics(sparkSession)
    ontStat.GetStatistics(O1)

  }

  def ResolveConflictClasses(sourceClassesWithBestTranslation: RDD[(String, String, String)], targetClassesWithoutCodes: RDD[String], listOfMatchedClasses: RDD[List[String]]): RDD[(String, String, String)] = {

    val p = new PreProcessing()

    val multilingualMatchedClasses: RDD[(String, String, String)] = listOfMatchedClasses.map(x => (p.stringPreProcessing(x.head).split(" ", 2).last, x(1))).keyBy(_._1).leftOuterJoin(sourceClassesWithBestTranslation.keyBy(_._3)).map { case (sourceTranslation, ((sourceTranslation1, target), Some((code, source, sourceTranslation2)))) => (code, source, target) }

    val allClassesWithMultilingualInfo = this.TranslateResource(sourceClassesWithBestTranslation, multilingualMatchedClasses)

    allClassesWithMultilingualInfo
  }

  def ResolveConflictRelations(similarRelations: RDD[(String, String, String, String, Double)], relationsWithTranslation: RDD[(String, String, String)]): RDD[(String, String, String)] = {
    val multilingualMatchedRelations: RDD[(String, String, String)] = similarRelations.map(x => (x._1, x._2, x._4))
    val allRelationsWithMultilingualInfo = this.TranslateResource(relationsWithTranslation, multilingualMatchedRelations)
    allRelationsWithMultilingualInfo
  }

  def TranslateResource(listOfSourceResourcesWithTranslations: RDD[(String, String, String)], multilingualMatchedResources: RDD[(String, String, String)]): RDD[(String, String, String)] = {

    val allResources = listOfSourceResourcesWithTranslations.keyBy(_._1).leftOuterJoin(multilingualMatchedResources.keyBy(_._1)).map { case x => if (!x._2._2.isEmpty) (x._1, x._2._1._2, x._2._2.last._3) else (x._1, x._2._1._2, x._2._1._3) }

    allResources

  }

  def GenerateURIsForResources(allResourcesWithMultilingualInfo: RDD[(String, String)], resourceType: Char): RDD[(String, String, String)] = {
    val p = new PreProcessing()
    var resourcesWithURIs = sparkSession.sparkContext.emptyRDD[(String, String, String)]

    if (resourceType == 'C') resourcesWithURIs = allResourcesWithMultilingualInfo.map(x => ("https://merged-ontology#" ++ p.ToCamelForClass(x._2), x._1, x._2)) else if (resourceType == 'P') resourcesWithURIs = allResourcesWithMultilingualInfo.map(x => ("https://merged-ontology#" ++ p.ToCamelForRelation(x._2), x._1, x._2))
    resourcesWithURIs
  }

  def TranslateSourceOntology(resourcesWithURIs: RDD[(String, String, String)], sOntology: RDD[(String, String, String)]): RDD[graph.Triple] = {
    val sub = sOntology.keyBy(_._1).join(resourcesWithURIs.keyBy(_._2)).map { case (gr1, ((gr2, label, typ), ((uri, gr3, en)))) => (uri, label, typ) }
    val translatedOntology: RDD[(String, String, String)] = sub.keyBy(_._3).leftOuterJoin(resourcesWithURIs.keyBy(_._2)).map { case (gr1, ((uri1, label, typ), list)) => if (!list.isEmpty) (uri1, label, list.last._1) else (uri1, label, typ) }
    translatedOntology.filter(x => x._2 == "type").foreach(println(_))
    gCreate.CreateGraph(translatedOntology)
  }


  //  def GenerateRundomNumbersWithoutRepitition(len: Int): Array[Int] = {
  //    val rand = new Random()
  //    var randomNembers: Array[Int] = new Array[Int](len)
  //    randomNembers(0) = 100000 + rand.nextInt(900000)
  //    for (i <- 1 to (randomNembers.length - 1)) {
  //      randomNembers(i) = randomNembers(i - 1) + 1
  //    }
  ////    randomNembers.foreach(println(_))
  //    randomNembers
  //  }
}
