import org.apache.jena.graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class MultilingualOntology(sparkSession: SparkSession) extends Serializable {
  val gCreate = new GraphCreating()

  /**
    * Generates the multilingual merged ontology from O1 and O2 in two different natural languages.
    */
  def GenerateMultilingualOntology(O1ClassesWithTranslation: RDD[(String, String)], matchedClasses: RDD[(String, String, String, Double)], matchedRelations: RDD[(String, String, String, Double)], O1RelationsWithTranslation: RDD[(String, String)], O1Ontology: RDD[(String, String, String)], O2Ontology: RDD[(String, String, String)], O2translated: String): RDD[graph.Triple] = {

    val multilingualMatchedClasses = matchedClasses.map(x => (x._1, x._3))
    val multilingualMatchedRelations = matchedRelations.map(x => (x._1, x._3))


    val O1ClassesWithMultilingualInfo = this.ResolveConflictClasses(multilingualMatchedClasses, O1ClassesWithTranslation)

    val O1RelationsWithMultilingualInfo = this.ResolveConflictRelations(multilingualMatchedRelations, O1RelationsWithTranslation)

    val O1ClassesWithURIs: RDD[(String, String, String)] = this.GenerateURIsForResources(O1ClassesWithMultilingualInfo, 'C')


    val O1RelationsWithURIs = this.GenerateURIsForResources(O1RelationsWithMultilingualInfo, 'P')


    val translatedO1Ontology: RDD[graph.Triple] = this.TranslateNonEnglishOntology(O1ClassesWithURIs.union(O1RelationsWithURIs), O1Ontology)


    val O1EnglishLabels = gCreate.CreateMultilingualEnglishLabels(O1ClassesWithURIs.union(O1RelationsWithURIs))

    val O1GermanLabels = gCreate.CreateMultilingualGermanLabels(O1ClassesWithURIs.union(O1RelationsWithURIs))

    val O1 = translatedO1Ontology.union(O1EnglishLabels).union(O1GermanLabels)


    var O2ClassesWithMultilingualInfo: RDD[(String, String)] = sparkSession.sparkContext.textFile("src/main/resources/Output/" + O2translated + "/classesWithTranslation.txt").map(x => (x.split(",").apply(0).toLowerCase, x.split(",").apply(1).toLowerCase))
    var O2RelationsWithMultilingualInfo: RDD[(String, String)] = sparkSession.sparkContext.textFile("src/main/resources/Output/" + O2translated + "/RelationsWithTranslation.txt").map(x => (x.split(",").apply(0).toLowerCase, x.split(",").apply(1).toLowerCase))


    println("Numbers of classes and properties in " + O2translated + " are " + O2ClassesWithMultilingualInfo.count() + " " + O2RelationsWithMultilingualInfo.count())

    //swap english-german to german-english
    val O2ClassesWithURIs = this.GenerateURIsForResources(this.ResolveConflictsForEnglish(O2ClassesWithMultilingualInfo, multilingualMatchedClasses), 'C')

    val O2RelationsWithURIs = this.GenerateURIsForResources(this.ResolveConflictsForEnglish(O2RelationsWithMultilingualInfo, multilingualMatchedRelations), 'P')

    println("Numbers of classes and properties in " + O2translated + " after resolving conflicts " + O2ClassesWithURIs.count() + " " + O2RelationsWithURIs.count())

    val translatedO2Ontology: RDD[graph.Triple] = this.TranslateEnglishOntology(O2ClassesWithURIs.union(O2RelationsWithURIs), O2Ontology).distinct()


    val O2EnglishLabels = gCreate.CreateMultilingualEnglishLabels(O2ClassesWithURIs.union(O2RelationsWithURIs))


    val O2GermanLabels = gCreate.CreateMultilingualGermanLabels(O2ClassesWithURIs.union(O2RelationsWithURIs))


    val O2 = translatedO2Ontology.union(O2EnglishLabels).union(O2GermanLabels)


    val mergedOntology: RDD[graph.Triple] = O1.union(O2).distinct(2)

    //    mergedOntology.coalesce(1, shuffle = true).saveAsNTriplesFile("src/main/resources/Output/mergedOntology")
    mergedOntology
  }


  //  def GetMultilingualMatchedClasses(sourceClassesWithBestTranslation: RDD[(String, String)], listOfMatchedClasses: RDD[List[String]]): RDD[(String, String)]={
  //    val p = new PreProcessing()
  //    val multilingualMatchedClasses: RDD[(String, String)] = listOfMatchedClasses.map(x => (p.stringPreProcessing(x.head).split(" ", 2).last, x(1))).keyBy(_._1).leftOuterJoin(sourceClassesWithBestTranslation.keyBy(_._2)).map { case (sourceTranslation, ((sourceTranslation1, target), Some((source, sourceTranslation2)))) => (source, target) }
  //    multilingualMatchedClasses
  //  }
  //  def GetMultilingualMatchedRelations(similarRelations: RDD[(String, String, String)]): RDD[(String, String)]={
  //    val multilingualMatchedRelations: RDD[(String, String)] = similarRelations.map(x => (x._1, x._3))
  //    multilingualMatchedRelations
  //  }
  /**
    * Remove duplicated classes according to the matching results.
    */
  def ResolveConflictClasses(multilingualMatchedClasses: RDD[(String, String)], sourceClassesWithBestTranslation: RDD[(String, String)]): RDD[(String, String)] = {

    val allClassesWithMultilingualInfo: RDD[(String, String)] = this.TranslateResourceToEnglish(sourceClassesWithBestTranslation, multilingualMatchedClasses)
    allClassesWithMultilingualInfo

  }

  /**
    * Remove duplicated relations according to the matching results.
    */
  def ResolveConflictRelations(multilingualMatchedRelations: RDD[(String, String)], relationsWithTranslation: RDD[(String, String)]) = {
    val allRelationsWithMultilingualInfo = this.TranslateResourceToEnglish(relationsWithTranslation, multilingualMatchedRelations)
    allRelationsWithMultilingualInfo
  }

  def TranslateResourceToEnglish(listOfResourcesWithTranslations: RDD[(String, String)], multilingualMatchedResources: RDD[(String, String)]): RDD[(String, String)] = {
    val allResources: RDD[(String, String)] = listOfResourcesWithTranslations.keyBy(_._1).leftOuterJoin(multilingualMatchedResources.keyBy(_._1)).map { case x => if (x._2._2.isEmpty) (x._1, x._2._1._2) else (x._1, x._2._2.last._2) }
    allResources

  }

  def ResolveConflictsForEnglish(listOfResourcesWithTranslations: RDD[(String, String)], multilingualMatchedResources: RDD[(String, String)]): RDD[(String, String)] = {
    val allResources: RDD[(String, String)] = listOfResourcesWithTranslations.keyBy(_._1).leftOuterJoin(multilingualMatchedResources.keyBy(_._2)).map { case x => if (!x._2._2.isEmpty) (x._2._2.last._1, x._1) else (x._2._1._2, x._1) }.distinct().subtract(multilingualMatchedResources)

    //      .map { case x => if (x._2._2.isEmpty) (x._1, x._2._1._2) else (x._1, x._2._2.last._2) }
    allResources

  }

  /**
    * Generates new URIs for resources in the merged ontology.
    */
  def GenerateURIsForResources(allResourcesWithMultilingualInfo: RDD[(String, String)], resourceType: Char): RDD[(String, String, String)] = {
    val p = new PreProcessing()
    var resourcesWithURIs = sparkSession.sparkContext.emptyRDD[(String, String, String)]

    if (resourceType == 'C') resourcesWithURIs = allResourcesWithMultilingualInfo.map(x => ("https://merged-ontology#" ++ p.ToCamelForClass(x._2), x._1, x._2)) else if (resourceType == 'P') resourcesWithURIs = allResourcesWithMultilingualInfo.map(x => ("https://merged-ontology#" ++ p.ToCamelForRelation(x._2), x._1, x._2))
    resourcesWithURIs
  }

  def TranslateNonEnglishOntology(resourcesWithURIs: RDD[(String, String, String)], sOntology: RDD[(String, String, String)]): RDD[graph.Triple] = {
    val sub = sOntology.keyBy(_._1).join(resourcesWithURIs.keyBy(_._2)).map { case (gr1, ((gr2, label, typ), ((uri, gr3, en)))) => (uri, label, typ) }
    val translatedOntology: RDD[(String, String, String)] = sub.keyBy(_._3).leftOuterJoin(resourcesWithURIs.keyBy(_._2)).map { case (gr1, ((uri1, label, typ), list)) => if (!list.isEmpty) (uri1, label, list.last._1) else (uri1, label, typ) }

    gCreate.CreateGraph(translatedOntology)
  }

  def TranslateEnglishOntology(resourcesWithURIs: RDD[(String, String, String)], sOntology: RDD[(String, String, String)]): RDD[graph.Triple] = {
    val p = new PreProcessing()
    val sub = sOntology.keyBy(x => p.stringPreProcessing(p.splitCamelCase(x._1).toLowerCase.replaceAll("\\s{2,}", " ").trim())).leftOuterJoin(resourcesWithURIs.keyBy(_._3.toLowerCase)).filter(x => !x._2._2.isEmpty).map { case (en1, ((en2, relation, obj), Some((uri, gr, en3)))) => (uri, relation, obj) }

    val translatedOntology = sub.keyBy(x => p.stringPreProcessing(p.splitCamelCase(x._3).toLowerCase.replaceAll("\\s{2,}", " ").trim())).leftOuterJoin(resourcesWithURIs.keyBy(_._3.toLowerCase)).map { case (en1, ((uri1, relation, obj), list)) => if (!list.isEmpty) (uri1, relation, list.last._1) else (uri1, relation, p.stringPreProcessing2(obj)) }

    //    println("translated target ontology before graph"+translatedOntology.count())
    //    translatedOntology.foreach(println(_))
    gCreate.CreateGraph(translatedOntology)

  }

  //  def MultilingualInfoForTargetOntology(offlineDictionaryForTarget: String, resourceType: Char): RDD[(String, String)]= {
  //    var targetResourcesWithTranslation = sparkSession.sparkContext.emptyRDD[(String, String)]
  //    val targetDictionary: RDD[List[String]] = sparkSession.sparkContext.textFile(offlineDictionaryForTarget).map(_.split(",").toList)
  //    if (resourceType == 'C')
  //      targetResourcesWithTranslation = targetDictionary.filter(x => x(2) == "C").map(y => (y(0),y(1)))
  //    else if (resourceType == 'P')
  //      targetResourcesWithTranslation = targetDictionary.filter(x => x(2) == "P").map(y => (y(0),y(1)))
  //
  //    targetResourcesWithTranslation
  //  }
}
