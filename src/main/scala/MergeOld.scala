import org.apache.jena.graph
import org.apache.jena.graph.Node
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

class MergeOld(sparkSession1: SparkSession) {
  /**
    *  MergeOntologies two ontologies in two different natural languages.
    */
  def MergeOntologies(sourceOntology: RDD[graph.Triple], targetOntology: RDD[graph.Triple], offlineDictionaryForSource: String, offlineDictionaryForTarget: String): RDD[graph.Triple] ={
    val ontStat = new OntologyStatistics(sparkSession1)
//    ontStat.getStatistics(sourceOntology)
//    ontStat.getStatistics(targetOntology)
    val ontoRebuild = new OntologyRebuilding(sparkSession1)
    val p = new PreProcessing()


    val sOntology: RDD[(String, String, String)] = ontoRebuild.RebuildOntology(sourceOntology)
//    val tOntology = ontoRebuild.RebuildOntologyWithoutCodes(targetOntology)
val tOntology = ontoRebuild.RebuildOntology(targetOntology)

      //    println("======================================")
//    println("|     Resources Extraction Phase     |")
//    println("======================================")

    // Retrieve class and relation labels for source and target ontology
    val targetClassesWithoutCodes: RDD[String] = ontStat.getAllClasses(targetOntology).map(x => p.stringPreProcessing(x)).persist(StorageLevel.MEMORY_AND_DISK) //For SEO
    //    val targetClassesWithoutCodes: RDD[(String)] = ontStat.retrieveClassesWithCodesAndLabels(targetOntology).map(x=>x._2).persist(StorageLevel.MEMORY_AND_DISK) //For Cmt and Multifarm dataset

    val targetRelationsWithoutCodes: RDD[(String)] = ontStat.getAllRelations(targetOntology).map(x => p.stringPreProcessing(x._1))
    println("targetRelationsWithoutCodes")
    targetRelationsWithoutCodes.foreach(println(_))

    val sourceClassesWithCodes: RDD[(String, String)] = ontStat.retrieveClassesWithCodesAndLabels(sourceOntology) //applied for ontologies with codes like Multifarm ontologies

    val sourceOntologyLabels: Map[Node, graph.Triple] = sourceOntology.filter(x => x.getPredicate.getLocalName == "label").keyBy(_.getSubject).collect().toMap

    val sourceLabelBroadcasting: Broadcast[Map[Node, graph.Triple]] = sparkSession1.sparkContext.broadcast(sourceOntologyLabels)

    val sourceRelations: RDD[(String, String)] = ontStat.retrieveRelationsWithCodes(sourceLabelBroadcasting, sourceOntology)

//    println("======================================")
//    println("|    Cross-lingual Matching Phase    |")
//    println("======================================")
    // ####################### Class Translation using offline dictionaries from Google translate #######################
    val tc = targetClassesWithoutCodes.zipWithIndex().collect().toMap
    val targetClassesBroadcasting: Broadcast[Map[String, Long]] = sparkSession1.sparkContext.broadcast(tc)
    val translate = new ClassSimilarity(sparkSession1)

    val availableTranslations: RDD[(String, List[String])] = translate.GettingAllAvailableTranslations(offlineDictionaryForSource)

    val sourceClassesWithListOfBestTranslations = availableTranslations.map(x => (x._1, x._2, translate.GetBestTranslationForClass(x._2, targetClassesBroadcasting))).persist(StorageLevel.MEMORY_AND_DISK)

    val listOfMatchedClasses: RDD[List[String]] = sourceClassesWithListOfBestTranslations.map(x => x._3.toString().toLowerCase.split(",").toList).filter(y => y.last.exists(_.isDigit)).persist(StorageLevel.MEMORY_AND_DISK)
    println("List of matched classes: "+listOfMatchedClasses.count())
    listOfMatchedClasses.foreach(println(_))

    val sourceClassesWithBestTranslation: RDD[(String, String, String)] = sourceClassesWithListOfBestTranslations.map(x => (x._1.toLowerCase, p.stringPreProcessing(x._3.head.toString.toLowerCase.split(",").head))).keyBy(_._1).join(sourceClassesWithCodes).map({ case (u, ((uu, tt), s)) => (u, s, tt.trim.replaceAll(" +", " ")) })

    // ####################### Relation Translation using offline dictionaries from Google translate #######################
    val relationsWithTranslation: RDD[(String, String, String)] = translate.GetTranslationForRelation(availableTranslations, sourceRelations)
    println("relationsWithTranslation")
    relationsWithTranslation.foreach(println(_))

    val relationSim = new RelationSimilarity()
    val similarRelations: RDD[(String, String, String, String, Double)] = relationSim.GetRelationSimilarity(targetRelationsWithoutCodes, relationsWithTranslation)
    println("list of matched relations: "+similarRelations.count())
    similarRelations.foreach(println(_))


    val om = new MultilingualOntology(sparkSession1)
    val multilingualMergedOntology: RDD[graph.Triple] = om.GenerateMultilingualOntology(sourceClassesWithBestTranslation.map(x => (x._2, x._3)), listOfMatchedClasses, similarRelations.map(x => (x._2, x._3, x._4)), relationsWithTranslation.map(x => (x._2, x._3)), sOntology, tOntology, offlineDictionaryForTarget)

    println("==========================================================================")
    println("|         Quality Assessment for the merged ontology        |")
    println("==========================================================================")

    val quality = new QualityAssessment(sparkSession1)
    println("Class coverage for merged ontology Om is " + quality.ClassCoverage(sourceOntology, targetOntology, multilingualMergedOntology, listOfMatchedClasses.count().toInt))
    println("Property coverage for merged ontology Om is " + quality.PropertyCoverage(sourceOntology, targetOntology, multilingualMergedOntology, similarRelations.count().toInt))
    println("Compactness for merged ontology Om is " + quality.Compactness(sourceOntology, targetOntology, multilingualMergedOntology))


    multilingualMergedOntology
  }


}
