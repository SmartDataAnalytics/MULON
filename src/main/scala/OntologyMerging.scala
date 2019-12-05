import org.apache.jena.graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


class OntologyMerging(sparkSession: SparkSession) extends Serializable {
  val gCreate = new GraphCreating()


  def Merge(sourceClassesWithBestTranslation: RDD[(String, String)], listOfMatchedClasses: RDD[List[String]], similarRelations: RDD[(String, String, String)], relationsWithTranslation: RDD[(String, String)], sOntology: RDD[(String, String, String)], tOntology: RDD[(String, String, String)], offlineDictionaryForTarget: String) = {

    val multilingualMatchedClasses = this.GetMultilingualMatchedClasses(sourceClassesWithBestTranslation,listOfMatchedClasses)
    val multilingualMatchedRelations = this.GetMultilingualMatchedRelations(similarRelations)


    val sourceClassesWithMultilingualInfo = this.ResolveConflictClasses(multilingualMatchedClasses, sourceClassesWithBestTranslation)

    val sourceRelationsWithMultilingualInfo = this.ResolveConflictRelations(multilingualMatchedRelations, relationsWithTranslation)

    println("All resources for source ontology with multilingual info after resolving conflicts:")
    sourceClassesWithMultilingualInfo.union(sourceRelationsWithMultilingualInfo).foreach(println(_))

    val sourceClassesWithURIs: RDD[(String, String, String)] = this.GenerateURIsForResources(sourceClassesWithMultilingualInfo, 'C')

    println("Source classes with URIs" + sourceClassesWithURIs.count())
    sourceClassesWithURIs.foreach(println(_))

    val sourceRelationsWithURIs = this.GenerateURIsForResources(sourceRelationsWithMultilingualInfo, 'P')

    println("Source relations with URIs" + sourceRelationsWithURIs.count())
    sourceRelationsWithURIs.foreach(println(_))

    val translatedSourceOntology: RDD[graph.Triple] = this.TranslateGermanOntology(sourceClassesWithURIs.union(sourceRelationsWithURIs), sOntology)
    println("Translate ontology with number of triples")
    translatedSourceOntology.foreach(println(_))

    val englishLabels = gCreate.CreateMultilingualEnglishLabels(sourceClassesWithURIs.union(sourceRelationsWithURIs)) //    println("English labels"+englishLabels.count())
    //    englishLabels.foreach(println(_))
    val germanLabels = gCreate.CreateMultilingualGermanLabels(sourceClassesWithURIs.union(sourceRelationsWithURIs)) //    println("German labels"+germanLabels.count())
    //    germanLabels.foreach(println(_))
    val O1 = translatedSourceOntology.union(englishLabels).union(germanLabels)
    println("Multilingual O1")
    O1.take(20).foreach(println(_))
    //    O1.coalesce(1, shuffle = true).saveAsNTriplesFile("src/main/resources/Output/O1")
    val ontStat = new OntologyStatistics(sparkSession)
    ontStat.GetStatistics(O1)
//###########################################################################################################
    val targetClassesWithMultilingualInfo = this.MultilingualInfoForTargetOntology(offlineDictionaryForTarget, 'C')
    println("targetClassesWithMultilingualInfo " + targetClassesWithMultilingualInfo.count())
    targetClassesWithMultilingualInfo.foreach(println(_))
    val targetRelationsWithMultilingualInfo = this.MultilingualInfoForTargetOntology(offlineDictionaryForTarget, 'P')
    println("targetRelationsWithMultilingualInfo " + targetRelationsWithMultilingualInfo.count())
    targetRelationsWithMultilingualInfo.foreach(println(_))

    //swap english-german to german-english
    val targetClassesWithURIs = this.GenerateURIsForResources(this.ResolveConflictsForEnglish(targetClassesWithMultilingualInfo,multilingualMatchedClasses), 'C')
    println("targetClassesWithURIs"+targetClassesWithURIs.count())
    targetClassesWithURIs.foreach(println(_))
    val targetRelationsWithURIs = this.GenerateURIsForResources(this.ResolveConflictsForEnglish(targetRelationsWithMultilingualInfo,multilingualMatchedRelations), 'P')
    println("targetRelationsWithURIs"+targetRelationsWithURIs.count())
    targetRelationsWithURIs.foreach(println(_))

    this.TranslateEnglishOntology(targetClassesWithURIs.union(targetRelationsWithURIs), tOntology)
//    val translatedTargetOntology: RDD[graph.Triple] = this.TranslateGermanOntology(targetClassesWithURIs.union(targetRelationsWithURIs), tOntology)
//    println("Translate target ontology with number of triples")
//    translatedTargetOntology.foreach(println(_))


  }

  def GetMultilingualMatchedClasses(sourceClassesWithBestTranslation: RDD[(String, String)], listOfMatchedClasses: RDD[List[String]]): RDD[(String, String)]={
    val p = new PreProcessing()
    val multilingualMatchedClasses: RDD[(String, String)] = listOfMatchedClasses.map(x => (p.stringPreProcessing(x.head).split(" ", 2).last, x(1))).keyBy(_._1).leftOuterJoin(sourceClassesWithBestTranslation.keyBy(_._2)).map { case (sourceTranslation, ((sourceTranslation1, target), Some((source, sourceTranslation2)))) => (source, target) }
    multilingualMatchedClasses
  }
  def GetMultilingualMatchedRelations(similarRelations: RDD[(String, String, String)]): RDD[(String, String)]={
    val multilingualMatchedRelations: RDD[(String, String)] = similarRelations.map(x => (x._1, x._3))
    multilingualMatchedRelations
  }


  def ResolveConflictClasses(multilingualMatchedClasses: RDD[(String, String)], sourceClassesWithBestTranslation: RDD[(String, String)]): RDD[(String, String)] = {

    val allClassesWithMultilingualInfo: RDD[(String, String)] = this.TranslateResourceToEnglish(sourceClassesWithBestTranslation, multilingualMatchedClasses)
    allClassesWithMultilingualInfo

  }


  def ResolveConflictRelations(multilingualMatchedRelations: RDD[(String, String)], relationsWithTranslation: RDD[(String, String)]) = {
    val allRelationsWithMultilingualInfo = this.TranslateResourceToEnglish(relationsWithTranslation, multilingualMatchedRelations)
    allRelationsWithMultilingualInfo
  }

  def TranslateResourceToEnglish(listOfResourcesWithTranslations: RDD[(String, String)], multilingualMatchedResources: RDD[(String, String)]): RDD[(String, String)] = {
    val allResources: RDD[(String, String)] = listOfResourcesWithTranslations.keyBy(_._1).leftOuterJoin(multilingualMatchedResources.keyBy(_._1)).map { case x => if (x._2._2.isEmpty) (x._1, x._2._1._2) else (x._1, x._2._2.last._2) }
    allResources

  }
  def ResolveConflictsForEnglish(listOfResourcesWithTranslations: RDD[(String, String)], multilingualMatchedResources: RDD[(String, String)]): RDD[(String, String)]= {
    val allResources: RDD[(String, String)] = listOfResourcesWithTranslations.keyBy(_._1).leftOuterJoin(multilingualMatchedResources.keyBy(_._2))
      .map{case x => if (!x._2._2.isEmpty) (x._2._2.last._1, x._1) else (x._2._1._2,x._1)}.distinct()
    println("Translate target to german")
    allResources.foreach(println(_))

//      .map { case x => if (x._2._2.isEmpty) (x._1, x._2._1._2) else (x._1, x._2._2.last._2) }
    allResources

  }

  def GenerateURIsForResources(allResourcesWithMultilingualInfo: RDD[(String, String)], resourceType: Char): RDD[(String, String, String)] = {
    val p = new PreProcessing()
    var resourcesWithURIs = sparkSession.sparkContext.emptyRDD[(String, String, String)]

    if (resourceType == 'C') resourcesWithURIs = allResourcesWithMultilingualInfo.map(x => ("https://merged-ontology#" ++ p.ToCamelForClass(x._2), x._1, x._2)) else if (resourceType == 'P') resourcesWithURIs = allResourcesWithMultilingualInfo.map(x => ("https://merged-ontology#" ++ p.ToCamelForRelation(x._2), x._1, x._2))
    resourcesWithURIs
  }

  def TranslateGermanOntology(resourcesWithURIs: RDD[(String, String, String)], sOntology: RDD[(String, String, String)]): RDD[graph.Triple] = {
    val sub = sOntology.keyBy(_._1).join(resourcesWithURIs.keyBy(_._2)).map { case (gr1, ((gr2, label, typ), ((uri, gr3, en)))) => (uri, label, typ) }
    val translatedOntology: RDD[(String, String, String)] = sub.keyBy(_._3).leftOuterJoin(resourcesWithURIs.keyBy(_._2)).map { case (gr1, ((uri1, label, typ), list)) => if (!list.isEmpty) (uri1, label, list.last._1) else (uri1, label, typ) }
    translatedOntology.filter(x => x._2 == "type").foreach(println(_))
    println("translated ontology before graph"+translatedOntology.count())
    translatedOntology.foreach(println(_))
    gCreate.CreateGraph(translatedOntology)
  }

  def TranslateEnglishOntology(resourcesWithURIs: RDD[(String, String, String)], sOntology: RDD[(String, String, String)])= {
    val sub = sOntology.keyBy(_._1).leftOuterJoin(resourcesWithURIs.keyBy(_._3))//.map { case (gr1, ((gr2, label, typ), ((uri, gr3, en)))) => (uri, label, typ) }
//    val translatedOntology: RDD[(String, String, String)] = sub.keyBy(_._3).leftOuterJoin(resourcesWithURIs.keyBy(_._2)).map { case (gr1, ((uri1, label, typ), list)) => if (!list.isEmpty) (uri1, label, list.last._1) else (uri1, label, typ) }
//    translatedOntology.filter(x => x._2 == "type").foreach(println(_))
    println("target ontology triples"+sOntology.count())
    println("translated ontology before graph"+sub.count())
    sub.foreach(println(_))
//    gCreate.CreateGraph(translatedOntology)
  }

  def MultilingualInfoForTargetOntology(offlineDictionaryForTarget: String, resourceType: Char): RDD[(String, String)]= {
    var targetResourcesWithTranslation = sparkSession.sparkContext.emptyRDD[(String, String)]
    val targetDictionary: RDD[List[String]] = sparkSession.sparkContext.textFile(offlineDictionaryForTarget).map(_.split(",").toList)
    if (resourceType == 'C')
      targetResourcesWithTranslation = targetDictionary.filter(x => x(3) == "C").map(y => (y(1),y(2)))
    else if (resourceType == 'P')
      targetResourcesWithTranslation = targetDictionary.filter(x => x(3) == "P").map(y => (y(1),y(2)))

    targetResourcesWithTranslation
  }

}
