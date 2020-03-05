import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.graph.Node
import org.apache.jena.riot.Lang
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Try {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder
      .master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
    val O1 = "src/main/resources/CaseStudy/SEO.ttl"
//    val O2 = "src/main/resources/EvaluationDataset/English/ekaw-en.ttl"


    val lang1: Lang = Lang.TURTLE
    val O1triples: RDD[graph.Triple] = sparkSession.rdf(lang1)(O1).distinct(2)
//    O1triples.filter(x=> x.getPredicate.getLocalName == "label").foreach(println(_))
    O1triples.foreach(println(_))
//    val O2triples: RDD[graph.Triple] = sparkSession.rdf(lang1)(O2).distinct(2)

    val ontoStat = new OntologyStatistics(sparkSession)
    ontoStat.getStatistics(O1triples)

    val O1Classes: RDD[String] = ontoStat.getAllClasses(O1triples) //ontoStat.retrieveClassesWithCodesAndLabels(O1triples).map(x => x._2)
    println("Classes of O1 are: "+O1Classes.count())
    O1Classes.foreach(println(_))
//    val O2Classes = ontoStat.getAllClasses(O2triples)//ontoStat.retrieveClassesWithoutLabels(O2triples)//.map(x => x._2)
//    println("Classes of O2 are: "+O2Classes.count())
//    O2Classes.foreach(println(_))


    val O1Labels: Map[Node, graph.Triple] = O1triples.filter(x => x.getPredicate.getLocalName == "label").keyBy(_.getSubject).collect().toMap
    val O1LabelsBroadcasting: Broadcast[Map[Node, graph.Triple]] = sparkSession.sparkContext.broadcast(O1Labels)

    val O1Relations: RDD[String] = ontoStat.retrieveRelationsWithCodes(O1LabelsBroadcasting, O1triples).map(x => x._2)
    println("Relations in O1 are: "+ O1Relations.count())
    O1Relations.foreach(println(_))

//    val O2Relations = ontoStat.getAllRelations(O2triples).map(x => x._1)
//    println("Relations in O2 are: "+ O2Relations.count())
//    O2Relations.foreach(println(_))

    val languageTag: String = O1triples.filter(x=> x.getPredicate.getLocalName == "label").first().getObject.getLiteralLanguage
    println("language tag is "+languageTag)




//    Translation.translateToEnglish(O1Classes,O1Relations, languageTag)



    //    val O1ClassesWithTranslation: RDD[(String, String)] = sparkSession.sparkContext.textFile("src/main/resources/Output/Conference-de/classesWithTranslation/classesWithTranslation.txt").map(x => (x.split(",").apply(0),x.split(",").apply(1)))
//    O1ClassesWithTranslation.foreach(println(_))
//
//    println("Classes Similarity:")
//    val sim = new TestClassSimilarity()
//    sim.GetSimilarity(O1ClassesWithTranslation,O2Classes)
//
//    val O1RelationsWithTranslation = sparkSession.sparkContext.textFile("src/main/resources/Output/Conference-de/RelationsWithTranslation/RelationsWithTranslation.txt").map(x => (x.split(",").apply(0),x.split(",").apply(1)))
////    println("Relations with translation")
////    O1RelationsWithTranslation.foreach(println(_))
//
//    println("Relations Similarity:")
//    val relSim = new RelationSimilarity()
//    relSim.GetRelationSimilarityTest(O2Relations,O1RelationsWithTranslation)


    sparkSession.stop()
  }
}
