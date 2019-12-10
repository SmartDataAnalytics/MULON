import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.graph.Node
import org.apache.jena.riot.Lang
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

//import net.sansa_stack.ml.common.nlp.wordnet
/*
* Created by Shimaa 15.oct.2018
* */ object Main {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkSession1 = SparkSession.builder //      .master("spark://172.18.160.16:3090")
      .master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()

//    val inputTarget = "src/main/resources/CaseStudy/SEO.nt"
//      val inputTarget = "src/main/resources/CaseStudy/ModSci-Copy.nt"
    val inputTarget = "src/main/resources/EvaluationDataset/English/edas-en.nt"
//    val inputTarget = "src/main/resources/EvaluationDataset/English/cmt-en.nt"
//    val inputTarget = "src/main/resources/EvaluationDataset/English/ekaw-en.nt"


//    val inputSource = "src/main/resources/EvaluationDataset/German/conference-de.nt"
    val inputSource = "src/main/resources/EvaluationDataset/German/confOf-de.nt"
//    val inputSource = "src/main/resources/EvaluationDataset/German/sigkdd-de.nt"


//    val offlineDictionaryForSource = "src/main/resources/OfflineDictionaries/Translations-conference-de.csv"
    val offlineDictionaryForSource = "src/main/resources/OfflineDictionaries/Translations-confOf-de.csv"


//    val offlineDictionaryForTarget: String = "src/main/resources/OfflineDictionaries/Translations-SEO-en.csv"
//    val offlineDictionaryForTarget: String = "src/main/resources/OfflineDictionaries/Translations-Ekaw-en.csv"
    val offlineDictionaryForTarget: String = "src/main/resources/OfflineDictionaries/Translations-Edas-en.csv"


    val lang1: Lang = Lang.NTRIPLES
    val sourceOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputSource).distinct(2)
    val targetOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputTarget).distinct(2)
    val runTime = Runtime.getRuntime

    //Get statistics for input ontologies
    val ontStat = new OntologyStatistics(sparkSession1) //    val o = ontStat.GetNumberOfSubClasses
    //    println("predicates")
    //    targetOntology.map(_.getPredicate.getLocalName).distinct().foreach(println(_))
    ontStat.GetStatistics(sourceOntology)
    ontStat.GetStatistics(targetOntology)
    //Replacing classes and properties with their labels
    val ontoRebuild = new OntologyRebuilding(sparkSession1)
    val p = new PreProcessing()

    val sOntology: RDD[(String, String, String)] = ontoRebuild.RebuildOntologyWithLabels(sourceOntology)
    //    println("ObjectProperty in source ontology")
    //    sOntology.filter(_._3 == "ObjectProperty").foreach(println(_))
    //    val tOntology: RDD[(String, String, String)] = ontoRebuild.RebuildOntologyWithLabels(targetOntology)
    //    tOntology.foreach(println(_))
    val tOntology = ontoRebuild.RebuildTargetOntologyWithoutCodes(targetOntology) //    println("Target ontology triples"+tOntology.count())
    //    tOntology.foreach(println(_))
    println("======================================")
    println("|     Resources Extraction Phase     |")
    println("======================================")

    // Retrieve class and relation labels for source and target ontology
    val targetClassesWithoutCodes: RDD[String] = ontStat.RetrieveClassesWithLabels(targetOntology).map(x => p.stringPreProcessing(x)).persist(StorageLevel.MEMORY_AND_DISK) //For SEO
    //    val targetClassesWithoutCodes: RDD[(String)] = ontStat.RetrieveClassesWithCodesAndLabels(targetOntology).map(x=>x._2).persist(StorageLevel.MEMORY_AND_DISK) //For Cmt and Multifarm dataset
    println("All classes in the target ontology:" + targetClassesWithoutCodes.count())
    targetClassesWithoutCodes.foreach(println(_))
    //    targetClassesWithoutCodes.map(x => p.stringPreProcessing(x).toLowerCase).coalesce(1, shuffle = true).saveAsTextFile("Output/TargetClasses")
    val targetRelationsWithoutCodes: RDD[(String)] = ontStat.RetrieveRelationsWithoutCodes(targetOntology).map(x => p.stringPreProcessing(x._1))
    println("All relations in the target ontology: " + targetRelationsWithoutCodes.count())
    targetRelationsWithoutCodes.foreach(println(_))
    //    targetRelationsWithoutCodes.map(x => p.splitCamelCase(x._1).toLowerCase).coalesce(1, shuffle = true).saveAsTextFile("Output/TargetRelations")
    val sourceClassesWithCodes: RDD[(String, String)] = ontStat.RetrieveClassesWithCodesAndLabels(sourceOntology) //applied for ontologies with codes like Multifarm ontologies
    println("All classes in the source ontology:" + sourceClassesWithCodes.count())
    sourceClassesWithCodes.foreach(println(_))
    //    sourceClassesWithCodes.map(x => x._2.toLowerCase).coalesce(1, shuffle = true).saveAsTextFile("src/main/resources/Output/SourceClasses")
    val sourceOntologyLabels: Map[Node, graph.Triple] = sourceOntology.filter(x => x.getPredicate.getLocalName == "label").keyBy(_.getSubject).collect().toMap
    //    println("All labels in the source ontology")
    //    sourceOntologyLabels.foreach(println(_))
    val sourceLabelBroadcasting: Broadcast[Map[Node, graph.Triple]] = sparkSession1.sparkContext.broadcast(sourceOntologyLabels)

    val sourceRelations: RDD[(String, String)] = ontStat.RetrieveRelationsWithCodes(sourceLabelBroadcasting, sourceOntology)
    println("All relations in the source ontology: " + sourceRelations.count())
    sourceRelations.foreach(println(_)) //    sourceRelations.map(x => x._2.toLowerCase).coalesce(1, shuffle = true).saveAsTextFile("src/main/resources/Output/SourceRelations")
    println("======================================")
    println("|    Cross-lingual Matching Phase    |")
    println("======================================")
    //    println("1) Translation using offline dictionaries:")
    // ####################### Class Translation using offline dictionaries from Google translate #######################
    val tc = targetClassesWithoutCodes.zipWithIndex().collect().toMap
    val targetClassesBroadcasting: Broadcast[Map[String, Long]] = sparkSession1.sparkContext.broadcast(tc)
    val translate = new ClassSimilarity(sparkSession1)
    val availableTranslations: RDD[(String, List[String])] = translate.GettingAllAvailableTranslations(offlineDictionaryForSource) //    println("All available translations:")
    //    availableTranslations.foreach(println(_))
    val sourceClassesWithListOfBestTranslations = availableTranslations.map(x => (x._1, x._2, translate.GetBestTranslationForClass(x._2, targetClassesBroadcasting))).persist(StorageLevel.MEMORY_AND_DISK) //        println("All source classes with list of best translations ")
    //        sourceClassesWithListOfBestTranslations.foreach(println(_))
    //sourceClassesWithListOfBestTranslations.coalesce(1).saveAsTextFile("src/main/resources/EvaluationDataset/German/translation")
    println("2) Cross-lingual semantic similarity:")
    val listOfMatchedClasses: RDD[List[String]] = sourceClassesWithListOfBestTranslations.map(x => x._3.toString().toLowerCase.split(",").toList).filter(y => y.last.exists(_.isDigit)).persist(StorageLevel.MEMORY_AND_DISK)
    println("List of matched classes between source and target ontologies: " + listOfMatchedClasses.count())
    listOfMatchedClasses.map(x => (x(0), x(1), x(2))).foreach(println(_))

    val sourceClassesWithBestTranslation: RDD[(String, String, String)] = sourceClassesWithListOfBestTranslations.map(x => (x._1.toLowerCase, p.stringPreProcessing(x._3.head.toString.toLowerCase.split(",").head))).keyBy(_._1).join(sourceClassesWithCodes).map({ case (u, ((uu, tt), s)) => (u, s, tt.trim.replaceAll(" +", " ")) }) //.cache()//.filter(!_.isDigit)
    //        println("Source classes with the best translation W.R.T the target ontology: ")
    //        sourceClassesWithBestTranslation.foreach(println(_))
    // ####################### Relation Translation using offline dictionaries from Google translate #######################
    val relationsWithTranslation: RDD[(String, String, String)] = translate.GetTranslationForRelation(availableTranslations, sourceRelations)
    //    println("Relations with translations: ")
    //    relationsWithTranslation.foreach(println(_))
    //    println("Semantic similarity for relations: ")
    val relationSim = new RelationSimilarity()
    val similarRelations: RDD[(String, String, String, String, Double)] = relationSim.GetRelationSimilarity(targetRelationsWithoutCodes, relationsWithTranslation)
    println("Relation similarity: ")
    similarRelations.foreach(println(_))

    val om = new OntologyMerging(sparkSession1)
    val multilingualMergedOntology = om.Merge(sourceClassesWithBestTranslation.map(x => (x._2, x._3)), listOfMatchedClasses, similarRelations.map(x => (x._2, x._3, x._4)), relationsWithTranslation.map(x => (x._2, x._3)), sOntology, tOntology, offlineDictionaryForTarget)

    println("======================================")
    println("|         Quality Assessment         |")
    println("======================================")
    val quality = new QualityAssessment(sparkSession1)
    println("Relationship richness for O1 is " + quality.RelationshipRichness(sourceOntology))
    println("Relationship richness for O2 is " + quality.RelationshipRichness(targetOntology))
    println("Relationship richness for Om is " + quality.RelationshipRichness(multilingualMergedOntology))
    println("==============================================")
    println("Attribute richness for O1 is " + quality.AttributeRichness(sourceOntology))
    println("Attribute richness for O2 is " + quality.AttributeRichness(targetOntology))
    println("Attribute richness for Om is " + quality.AttributeRichness(multilingualMergedOntology))
    println("==============================================")
    println("Inheritance richness for O1 is " + quality.InheritanceRichness(sourceOntology))
    println("Inheritance richness for O2 is " + quality.InheritanceRichness(targetOntology))
    println("Inheritance richness for Om is " + quality.InheritanceRichness(multilingualMergedOntology))
    println("==============================================")
    println("Readability for O1 is " + quality.Readability(sourceOntology))
    println("Readability for O2 is " + quality.Readability(targetOntology))
    println("Readability for Om is " + quality.Readability(multilingualMergedOntology))
    println("==============================================")
    println("Isolated Elements for O1 is " + quality.IsolatedElements(sourceOntology))
    println("Isolated Elements for O2 is " + quality.IsolatedElements(targetOntology))
    println("Isolated Elements for Om is " + quality.IsolatedElements(multilingualMergedOntology))
    println("==============================================")
    println("Missing Domain Or Range for O1 is " + quality.MissingDomainOrRange(sourceOntology))
    println("Missing Domain Or Range for O2 is " + quality.MissingDomainOrRange(targetOntology))
    println("Missing Domain Or Range for Om is " + quality.MissingDomainOrRange(multilingualMergedOntology))
    println("==============================================")
    println("Class coverage for merged ontology Om is " + quality.ClassCoverage(sourceOntology, targetOntology, multilingualMergedOntology, listOfMatchedClasses.count().toInt))
    println("Property coverage for merged ontology Om is " + quality.PropertyCoverage(sourceOntology, targetOntology, multilingualMergedOntology, similarRelations.count().toInt))
    println("Compactness for merged ontology Om is " + quality.Compactness(sourceOntology, targetOntology, multilingualMergedOntology))
    sparkSession1.stop
  }
}
