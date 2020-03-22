import org.apache.jena.graph
import org.apache.jena.graph.Node
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

class Merge(sparkSession1: SparkSession) {
  /**
    * Merge two ontologies in two different natural languages.
    */
  def MergeOntologies(O1triples: RDD[graph.Triple], O2triples: RDD[graph.Triple], offlineDictionaryForO1: String, offlineDictionaryForO2: String)= {
    val ontStat = new OntologyStatistics(sparkSession1)
    //    ontStat.getStatistics(O1triples)
    //    ontStat.getStatistics(O2triples)
    val ontoRebuild = new OntologyRebuilding(sparkSession1)
    val p = new PreProcessing()


    val sOntology: RDD[(String, String, String)] = ontoRebuild.RebuildOntology(O1triples)
    //    val tOntology = ontoRebuild.RebuildOntologyWithoutCodes(O2triples)
    val tOntology = ontoRebuild.RebuildOntology(O2triples)

    //    println("======================================")
    //    println("|     Resources Extraction Phase     |")
    //    println("======================================")
    // Retrieve class and relation labels for input ontologies
    //    val O1Classes: RDD[(String, String)] = ontStat.retrieveClassesWithCodesAndLabels(O1triples) //applied for ontologies with codes like Multifarm ontologies
    val O1Classes: RDD[String] = ontStat.getAllClasses(O1triples) //applied for ontologies with codes like Multifarm ontologies
    println("All classes in O1:")
    O1Classes.foreach(println(_))
    val O1Labels: Map[Node, graph.Triple] = O1triples.filter(x => x.getPredicate.getLocalName == "label").keyBy(_.getSubject).collect().toMap
    val O1LabelsBroadcasting: Broadcast[Map[Node, graph.Triple]] = sparkSession1.sparkContext.broadcast(O1Labels)
    val O1Relations: RDD[String] = ontStat.getAllRelations(O1LabelsBroadcasting, O1triples).map(x => x._2)
    println("All relations in O1")
    O1Relations.foreach(println(_))

    val O2Classes: RDD[String] = ontStat.getAllClasses(O2triples).map(x => p.stringPreProcessing(x)).persist(StorageLevel.MEMORY_AND_DISK) //For SEO
    //    val O2Classes: RDD[(String)] = ontStat.retrieveClassesWithCodesAndLabels(O2triples).map(x=>x._2).persist(StorageLevel.MEMORY_AND_DISK) //For Cmt and Multifarm dataset
    println("All classes in O2")
    O2Classes.foreach(println(_))
    //      val O2Relations: RDD[(String)] = ontStat.getAllRelationsOld(O2triples).map(x => p.stringPreProcessing(x._1))
    val O2Labels: Map[Node, graph.Triple] = O2triples.filter(x => x.getPredicate.getLocalName == "label").keyBy(_.getSubject).collect().toMap
    val O2LabelsBroadcasting: Broadcast[Map[Node, graph.Triple]] = sparkSession1.sparkContext.broadcast(O2Labels)
    val O2Relations: RDD[(String)] = ontStat.getAllRelations(O2LabelsBroadcasting, O2triples).map(x => p.stringPreProcessing(x._1))
    println("All relations in O2")
    O2Relations.foreach(println(_))

    //    println("======================================")
    //    println("|    Cross-lingual Matching Phase    |")
    //    println("======================================")
    // ####################### Automatic Translation using Yandex API #######################
//    val languageTag1: String = O1triples.filter(x=> x.getPredicate.getLocalName == "label").first().getObject.getLiteralLanguage
//    println("language tag for O1 is "+languageTag1)
//        Translation.translateToEnglish(O1Classes,O1Relations, languageTag1)

//        val languageTag2: String = O2triples.filter(x=> x.getPredicate.getLocalName == "label").first().getObject.getLiteralLanguage
//        println("language tag for O2 is "+languageTag2)
//        Translation.translateToEnglish(O2Classes,O2Relations, languageTag2)

        val O1ClassesWithTranslation: RDD[(String, String)] = sparkSession1.sparkContext.textFile("src/main/resources/Output/Conference-de/classesWithTranslation.txt").map(x => (x.split(",").apply(0),x.split(",").apply(1)))
//    println("O1 classes with translation")
//    O1ClassesWithTranslation.foreach(println(_))

        println("Classes Similarity:")
        val sim = new TestClassSimilarity()
        sim.GetSimilarity(O1ClassesWithTranslation,O2Classes)

        val O1RelationsWithTranslation = sparkSession1.sparkContext.textFile("src/main/resources/Output/Conference-de/RelationsWithTranslation.txt").map(x => (x.split(",").apply(0),x.split(",").apply(1)))
//        println("O1 relations with translation")
//        O1RelationsWithTranslation.foreach(println(_))

        println("Relations Similarity:")
        val relSim = new RelationSimilarity()
        relSim.GetRelationSimilarityTest(O2Relations,O1RelationsWithTranslation)

    //To do:
//    get the matched terms for classes and relations separately



    /*


    val tc = O2Classes.zipWithIndex().collect().toMap
    val targetClassesBroadcasting: Broadcast[Map[String, Long]] = sparkSession1.sparkContext.broadcast(tc)
    val translate = new ClassSimilarity(sparkSession1)

    val availableTranslations: RDD[(String, List[String])] = translate.GettingAllAvailableTranslations(offlineDictionaryForO1)

    val sourceClassesWithListOfBestTranslations = availableTranslations.map(x => (x._1, x._2, translate.GetBestTranslationForClass(x._2, targetClassesBroadcasting))).persist(StorageLevel.MEMORY_AND_DISK)

    val listOfMatchedClasses: RDD[List[String]] = sourceClassesWithListOfBestTranslations.map(x => x._3.toString().toLowerCase.split(",").toList).filter(y => y.last.exists(_.isDigit)).persist(StorageLevel.MEMORY_AND_DISK)
    println("List of matched classes: " + listOfMatchedClasses.count())
    listOfMatchedClasses.foreach(println(_))

//    val sourceClassesWithBestTranslation: RDD[(String, String, String)] = sourceClassesWithListOfBestTranslations.map(x => (x._1.toLowerCase, p.stringPreProcessing(x._3.head.toString.toLowerCase.split(",").head))).keyBy(_._1).join(O1Classes).map({ case (u, ((uu, tt), s)) => (u, s, tt.trim.replaceAll(" +", " ")) })

    // ####################### Relation Translation using offline dictionaries from Google translate #######################
    val relationsWithTranslation: RDD[(String, String, String)] = translate.GetTranslationForRelation(availableTranslations, O1Relations)
    println("relationsWithTranslation")
    relationsWithTranslation.foreach(println(_))

    val relationSim = new RelationSimilarity()
    val similarRelations: RDD[(String, String, String, String, Double)] = relationSim.GetRelationSimilarity(O2Relations, relationsWithTranslation)
    println("list of matched relations: " + similarRelations.count())
    similarRelations.foreach(println(_))


    val om = new MultilingualOntology(sparkSession1)
    val multilingualMergedOntology: RDD[graph.Triple] = om.GenerateMultilingualOntology(sourceClassesWithBestTranslation.map(x => (x._2, x._3)), listOfMatchedClasses, similarRelations.map(x => (x._2, x._3, x._4)), relationsWithTranslation.map(x => (x._2, x._3)), sOntology, tOntology, offlineDictionaryForO2)

    println("==========================================================================")
    println("|         Quality Assessment for the merged ontology        |")
    println("==========================================================================")

    val quality = new QualityAssessment(sparkSession1)
    println("Class coverage for merged ontology Om is " + quality.ClassCoverage(O1triples, O2triples, multilingualMergedOntology, listOfMatchedClasses.count().toInt))
    println("Property coverage for merged ontology Om is " + quality.PropertyCoverage(O1triples, O2triples, multilingualMergedOntology, similarRelations.count().toInt))
    println("Compactness for merged ontology Om is " + quality.Compactness(O1triples, O2triples, multilingualMergedOntology))


    multilingualMergedOntology*/
  }


}
