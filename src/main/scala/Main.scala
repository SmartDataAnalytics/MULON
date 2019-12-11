import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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
    //    val inputTarget = "src/main/resources/EvaluationDataset/English/edas-en.nt"
    //    val inputTarget = "src/main/resources/EvaluationDataset/English/cmt-en.nt"
    val inputTarget = "src/main/resources/EvaluationDataset/English/ekaw-en.nt"


    val inputSource = "src/main/resources/EvaluationDataset/German/conference-de.nt" //    val inputSource = "src/main/resources/EvaluationDataset/German/confOf-de.nt"
    //    val inputSource = "src/main/resources/EvaluationDataset/German/sigkdd-de.nt"
    val offlineDictionaryForSource: String = "src/main/resources/OfflineDictionaries/Translations-conference-de.csv" //    val offlineDictionaryForSource = "src/main/resources/OfflineDictionaries/Translations-confOf-de.csv"
    //    val offlineDictionaryForTarget: String = "src/main/resources/OfflineDictionaries/Translations-SEO-en.csv"
    val offlineDictionaryForTarget: String = "src/main/resources/OfflineDictionaries/Translations-Ekaw-en.csv" //    val offlineDictionaryForTarget: String = "src/main/resources/OfflineDictionaries/Translations-Edas-en.csv"
    val lang1: Lang = Lang.NTRIPLES
    val sourceOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputSource).distinct(2)
    val targetOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputTarget).distinct(2)
    val runTime = Runtime.getRuntime

    val ontoMerge = new OntologyMerging(sparkSession1)


    val multilingualMergedOntology = ontoMerge.Merge(sourceOntology, targetOntology, offlineDictionaryForSource, offlineDictionaryForTarget)
    println("======================================")
    println("|            Merged Ontology         |")
    println("======================================")
    multilingualMergedOntology.take(10).foreach(println(_))

    val ontStat = new OntologyStatistics(sparkSession1)
    println("Statistics for merged ontology")
    ontStat.GetStatistics(multilingualMergedOntology)

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


    sparkSession1.stop
  }
}
