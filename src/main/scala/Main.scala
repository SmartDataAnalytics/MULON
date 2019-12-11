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

    val O1 = "src/main/resources/EvaluationDataset/German/conference-de.nt"
    //    val O1 = "src/main/resources/EvaluationDataset/German/confOf-de.nt"
    //    val O1 = "src/main/resources/EvaluationDataset/German/sigkdd-de.nt"

    //    val O2 = "src/main/resources/CaseStudy/SEO.nt"
    //    val O2 = "src/main/resources/EvaluationDataset/English/edas-en.nt"
    //    val O2 = "src/main/resources/EvaluationDataset/English/cmt-en.nt"
    val O2 = "src/main/resources/EvaluationDataset/English/ekaw-en.nt"

    val offlineDictionaryForO1: String = "src/main/resources/OfflineDictionaries/Translations-conference-de.csv" //    val offlineDictionaryForO1 = "src/main/resources/OfflineDictionaries/Translations-confOf-de.csv"
    //    val offlineDictionaryForO2: String = "src/main/resources/OfflineDictionaries/Translations-SEO-en.csv"
    val offlineDictionaryForO2: String = "src/main/resources/OfflineDictionaries/Translations-Ekaw-en.csv" //    val offlineDictionaryForO2: String = "src/main/resources/OfflineDictionaries/Translations-Edas-en.csv"
    val lang1: Lang = Lang.NTRIPLES
    val O1triples: RDD[graph.Triple] = sparkSession1.rdf(lang1)(O1).distinct(2)
    val O2triples: RDD[graph.Triple] = sparkSession1.rdf(lang1)(O2).distinct(2)
    val runTime = Runtime.getRuntime

    val ontoMerge = new OntologyMerging(sparkSession1)


    val multilingualMergedOntology = ontoMerge.Merge(O1triples, O2triples, offlineDictionaryForO1, offlineDictionaryForO2)
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
    println("Relationship richness for O1 is " + quality.RelationshipRichness(O1triples))
    println("Relationship richness for O2 is " + quality.RelationshipRichness(O2triples))
    println("Relationship richness for Om is " + quality.RelationshipRichness(multilingualMergedOntology))
    println("==============================================")
    println("Attribute richness for O1 is " + quality.AttributeRichness(O1triples))
    println("Attribute richness for O2 is " + quality.AttributeRichness(O2triples))
    println("Attribute richness for Om is " + quality.AttributeRichness(multilingualMergedOntology))
    println("==============================================")
    println("Inheritance richness for O1 is " + quality.InheritanceRichness(O1triples))
    println("Inheritance richness for O2 is " + quality.InheritanceRichness(O2triples))
    println("Inheritance richness for Om is " + quality.InheritanceRichness(multilingualMergedOntology))
    println("==============================================")
    println("Readability for O1 is " + quality.Readability(O1triples))
    println("Readability for O2 is " + quality.Readability(O2triples))
    println("Readability for Om is " + quality.Readability(multilingualMergedOntology))
    println("==============================================")
    println("Isolated Elements for O1 is " + quality.IsolatedElements(O1triples))
    println("Isolated Elements for O2 is " + quality.IsolatedElements(O2triples))
    println("Isolated Elements for Om is " + quality.IsolatedElements(multilingualMergedOntology))
    println("==============================================")
    println("Missing Domain Or Range for O1 is " + quality.MissingDomainOrRange(O1triples))
    println("Missing Domain Or Range for O2 is " + quality.MissingDomainOrRange(O2triples))
    println("Missing Domain Or Range for Om is " + quality.MissingDomainOrRange(multilingualMergedOntology))


    sparkSession1.stop
  }
}
