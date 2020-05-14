import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*
* Created by Shimaa 15.oct.2018
* */ object OntologyMerging {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkSession1 = SparkSession.builder //      .master("spark://172.18.160.16:3090")
      .master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()

    //German ontologies
//        val O1 = "src/main/resources/EvaluationDataset/German/conference-de.ttl"
//    val O1 = "src/main/resources/EvaluationDataset/German/cmt-de.ttl"
//    val O1 = "src/main/resources/EvaluationDataset/German/confOf-de.ttl"
    //            val O1 = "src/main/resources/EvaluationDataset/German/iasted-de.ttl"
    //        val O1 = "src/main/resources/EvaluationDataset/German/sigkdd-de.ttl"

    //Arabic ontologies
    //        val O1 = "src/main/resources/EvaluationDataset/Arabic/conference-ar.ttl"
//                val O1 = "src/main/resources/EvaluationDataset/Arabic/cmt-ar.ttl"
//                val O1 = "src/main/resources/EvaluationDataset/Arabic/confOf-ar.ttl"
    //            val O1 = "src/main/resources/EvaluationDataset/Arabic/iasted-ar.ttl"
    //        val O1 = "src/main/resources/EvaluationDataset/Arabic/sigkdd-ar.ttl"

    //French ontologies
    //            val O1 = "src/main/resources/EvaluationDataset/French/conference-fr.ttl"
    //          val O1 = "src/main/resources/EvaluationDataset/French/cmt-fr.ttl"
    //          val O1 = "src/main/resources/EvaluationDataset/French/confOf-fr.ttl"
//              val O1 = "src/main/resources/EvaluationDataset/French/iasted-fr.ttl"
              val O1 = "src/main/resources/EvaluationDataset/French/sigkdd-fr.ttl"

    //    val O2 = "src/main/resources/CaseStudy/SEO.ttl"
    //        val O2 = "src/main/resources/EvaluationDataset/English/cmt-en.ttl"
//    val O2 = "src/main/resources/EvaluationDataset/English/edas-en.ttl"
               val O2 = "src/main/resources/EvaluationDataset/English/ekaw-en.ttl"
    //    val offlineDictionaryForO1: String = "src/main/resources/OfflineDictionaries/Translations-conference-de.csv"
    //    val offlineDictionaryForO1 = "src/main/resources/OfflineDictionaries/Translations-confOf-de.csv"
    //        val offlineDictionaryForO2: String = "src/main/resources/OfflineDictionaries/Translations-SEO-en.csv"
//                val offlineDictionaryForO2: String = "src/main/resources/OfflineDictionaries/Translations-Ekaw-en.csv"
//    val offlineDictionaryForO2: String = "src/main/resources/OfflineDictionaries/Translations-Edas-en.csv"

    val lang1: Lang = Lang.TURTLE
    val O1triples: RDD[graph.Triple] = sparkSession1.rdf(lang1)(O1).distinct(2)
    val O2triples: RDD[graph.Triple] = sparkSession1.rdf(lang1)(O2).distinct(2)
    val runTime = Runtime.getRuntime

    val ontStat = new OntologyStatistics(sparkSession1) //    ontStat.getStatistics(O1triples)
    val ontoMerge = new Merge(sparkSession1)


    //    val multilingualMergedOntology = ontoMerge.MergeOntologies(O1triples, O2triples, offlineDictionaryForO1, offlineDictionaryForO2)
    val multilingualMergedOntology = ontoMerge.MergeOntologies(O1triples, O2triples,"Ekaw-fr", "Sigkdd-fr")
//val multilingualMergedOntology = ontoMerge.MergeOntologies(O1triples, O2triples,"src/main/resources/OfflineDictionaries/Translations-Edas-en.csv")


    println("======================================")
    println("|            Merged Ontology         |")
    println("======================================")
//    multilingualMergedOntology.take(10).foreach(println(_))

    println("Statistics for O1 ontology")
    ontStat.getStatistics(O1triples)

    println("Statistics for O2 ontology")
    ontStat.getStatistics(O2triples)

    println("Statistics for merged ontology")
    ontStat.getStatistics(multilingualMergedOntology)

    println("==========================================================================")
    println("|         Quality Assessment for each input and output ontologies        |")
    println("==========================================================================")
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
    println("==============================================")
    println("Redundancy in O1 is " + quality.Redundancy(O1triples))
    println("Redundancy in O2 is " + quality.Redundancy(O2triples))
    println("==============================================")
    println("Number of matched classes is "+ ontoMerge.numberOfMatchedClasses)
    println("Number of matched relations is "+ontoMerge.numberOfMatchedRelations)
    println("Class coverage for Om is " + quality.ClassCoverage(O1triples, O2triples, multilingualMergedOntology, ontoMerge.numberOfMatchedClasses))
    println("Property coverage for Om is " + quality.PropertyCoverage(O1triples, O2triples, multilingualMergedOntology, ontoMerge.numberOfMatchedRelations))
    println("Compactness for Om is " + quality.Compactness(O1triples, O2triples, multilingualMergedOntology))



    sparkSession1.stop
  }

}
