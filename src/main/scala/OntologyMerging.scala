import org.apache.jena.graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class OntologyMerging(sparkSession: SparkSession) extends Serializable {
  val gCreate = new GraphCreating()


  def GenerateMultilingualOntology(sourceClassesWithBestTranslation: RDD[(String, String)], listOfMatchedClasses: RDD[List[String]], similarRelations: RDD[(String, String, String)], relationsWithTranslation: RDD[(String, String)], sOntology: RDD[(String, String, String)], tOntology: RDD[(String, String, String)], offlineDictionaryForTarget: String): RDD[graph.Triple] = {

    val multilingualMatchedClasses = this.GetMultilingualMatchedClasses(sourceClassesWithBestTranslation,listOfMatchedClasses)
    val multilingualMatchedRelations = this.GetMultilingualMatchedRelations(similarRelations)


    val sourceClassesWithMultilingualInfo = this.ResolveConflictClasses(multilingualMatchedClasses, sourceClassesWithBestTranslation)

    val sourceRelationsWithMultilingualInfo = this.ResolveConflictRelations(multilingualMatchedRelations, relationsWithTranslation)

//    println("All resources for source ontology with multilingual info after resolving conflicts:")
//    sourceClassesWithMultilingualInfo.union(sourceRelationsWithMultilingualInfo).foreach(println(_))

    val sourceClassesWithURIs: RDD[(String, String, String)] = this.GenerateURIsForResources(sourceClassesWithMultilingualInfo, 'C')

//    println("Source classes with URIs" + sourceClassesWithURIs.count())
//    sourceClassesWithURIs.foreach(println(_))

    val sourceRelationsWithURIs = this.GenerateURIsForResources(sourceRelationsWithMultilingualInfo, 'P')

//    println("Source relations with URIs" + sourceRelationsWithURIs.count())
//    sourceRelationsWithURIs.foreach(println(_))

    val translatedSourceOntology: RDD[graph.Triple] = this.TranslateGermanOntology(sourceClassesWithURIs.union(sourceRelationsWithURIs), sOntology)
//    println("Translate ontology with number of triples")
//    translatedSourceOntology.foreach(println(_))

    val sourceEnglishLabels = gCreate.CreateMultilingualEnglishLabels(sourceClassesWithURIs.union(sourceRelationsWithURIs))
//        println("English labels"+sourceEnglishLabels.count())
//        sourceEnglishLabels.foreach(println(_))
    val sourceGermanLabels = gCreate.CreateMultilingualGermanLabels(sourceClassesWithURIs.union(sourceRelationsWithURIs))
//        println("German labels"+sourceGermanLabels.count())
//        sourceGermanLabels.foreach(println(_))
    val O1 = translatedSourceOntology.union(sourceEnglishLabels).union(sourceGermanLabels)
//    println("Multilingual O1")
//    O1.take(20).foreach(println(_))
    //    O1.coalesce(1, shuffle = true).saveAsNTriplesFile("src/main/resources/Output/O1")
    val ontStat = new OntologyStatistics(sparkSession)
    println("Statistics for first ontology")
    ontStat.GetStatistics(O1)
//###########################################################################################################
    val targetClassesWithMultilingualInfo = this.MultilingualInfoForTargetOntology(offlineDictionaryForTarget, 'C')
//    println("targetClassesWithMultilingualInfo " + targetClassesWithMultilingualInfo.count())
//    targetClassesWithMultilingualInfo.foreach(println(_))
    val targetRelationsWithMultilingualInfo = this.MultilingualInfoForTargetOntology(offlineDictionaryForTarget, 'P')
//    println("targetRelationsWithMultilingualInfo " + targetRelationsWithMultilingualInfo.count())
//    targetRelationsWithMultilingualInfo.foreach(println(_))

    //swap english-german to german-english
    val targetClassesWithURIs = this.GenerateURIsForResources(this.ResolveConflictsForEnglish(targetClassesWithMultilingualInfo,multilingualMatchedClasses), 'C')
//    println("targetClassesWithURIs"+targetClassesWithURIs.count())
//    targetClassesWithURIs.foreach(println(_))
    val targetRelationsWithURIs = this.GenerateURIsForResources(this.ResolveConflictsForEnglish(targetRelationsWithMultilingualInfo,multilingualMatchedRelations), 'P')
//    println("targetRelationsWithURIs"+targetRelationsWithURIs.count())
//    targetRelationsWithURIs.foreach(println(_))

    val translatedTargetOntology: RDD[graph.Triple] = this.TranslateEnglishOntology(targetClassesWithURIs.union(targetRelationsWithURIs), tOntology).distinct()
//    println("Translate target ontology with number of triples")
//    translatedTargetOntology.foreach(println(_))
//    translatedTargetOntology.coalesce(1, shuffle = true).saveAsNTriplesFile("src/main/resources/Output/translatedTargetOntology")

    val targetEnglishLabels = gCreate.CreateMultilingualEnglishLabels(targetClassesWithURIs.union(targetRelationsWithURIs))
//        println("English labels"+targetEnglishLabels.count())
//        targetEnglishLabels.foreach(println(_))

    val targetGermanLabels = gCreate.CreateMultilingualGermanLabels(targetClassesWithURIs.union(targetRelationsWithURIs))
//        println("German labels"+targetGermanLabels.count())
//        targetGermanLabels.foreach(println(_))

    val O2 = translatedTargetOntology.union(targetEnglishLabels).union(targetGermanLabels)
//    println("Multilingual O2")
//    O1.take(20).foreach(println(_))

    println("Statistics for second ontology")
    ontStat.GetStatistics(translatedTargetOntology)

    val mergedOntology: RDD[graph.Triple] = O1.union(O2).distinct(2)
    println("Statistics for merged ontology")
    ontStat.GetStatistics(mergedOntology)
//    mergedOntology.coalesce(1, shuffle = true).saveAsNTriplesFile("src/main/resources/Output/mergedOntology")
    mergedOntology
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
      .map{case x => if (!x._2._2.isEmpty) (x._2._2.last._1, x._1) else (x._2._1._2,x._1)}.distinct().subtract(multilingualMatchedResources)
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
//    translatedOntology.filter(x => x._2 == "type").foreach(println(_))
//    println("translated ontology before graph"+translatedOntology.count())
//    translatedOntology.foreach(println(_))
    gCreate.CreateGraph(translatedOntology)
  }

  def TranslateEnglishOntology(resourcesWithURIs: RDD[(String, String, String)], sOntology: RDD[(String, String, String)]): RDD[graph.Triple]= {
    val p = new PreProcessing()
    val sub = sOntology.keyBy(x=>p.stringPreProcessing(p.splitCamelCase(x._1).toLowerCase.replaceAll("\\s{2,}", " ").trim())).leftOuterJoin(resourcesWithURIs.keyBy(_._3.toLowerCase))
      .filter(x => !x._2._2.isEmpty)
      .map { case (en1, ((en2, relation, obj), Some((uri, gr, en3)))) => (uri, relation, obj) }

    val translatedOntology = sub.keyBy(x=>p.stringPreProcessing(p.splitCamelCase(x._3).toLowerCase.replaceAll("\\s{2,}", " ").trim())).leftOuterJoin(resourcesWithURIs.keyBy(_._3.toLowerCase)).map { case (en1, ((uri1, relation, obj), list)) => if (!list.isEmpty) (uri1, relation, list.last._1) else (uri1, relation, p.stringPreProcessing2(obj)) }

//    println("translated target ontology before graph"+translatedOntology.count())
//    translatedOntology.foreach(println(_))
    gCreate.CreateGraph(translatedOntology)

  }

  def MultilingualInfoForTargetOntology(offlineDictionaryForTarget: String, resourceType: Char): RDD[(String, String)]= {
    var targetResourcesWithTranslation = sparkSession.sparkContext.emptyRDD[(String, String)]
    val targetDictionary: RDD[List[String]] = sparkSession.sparkContext.textFile(offlineDictionaryForTarget).map(_.split(",").toList)
    if (resourceType == 'C')
      targetResourcesWithTranslation = targetDictionary.filter(x => x(2) == "C").map(y => (y(0),y(1)))
    else if (resourceType == 'P')
      targetResourcesWithTranslation = targetDictionary.filter(x => x(2) == "P").map(y => (y(0),y(1)))

    targetResourcesWithTranslation
  }

}
