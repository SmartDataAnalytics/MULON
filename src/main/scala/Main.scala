import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

//from flair.data import Sentence
//from flair.models import SequenceTagger
//from segtok.segmenter import split_single
//from flair.data import segtok_tokenizer

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkSession1 = SparkSession.builder
      //      .master("spark://172.18.160.16:3090")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
//    var inputSource = "src/main/resources/SEO.ttl"
    var inputSource = "src/main/resources/conference-de.nt"
    //    val lang1: Lang = Lang.TURTLE
    val lang1: Lang = Lang.NTRIPLES
    val sourceOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputSource)
    sourceOntology.foreach(println(_))
    println("################################")
//    val x = sourceOntology.map(y => if (y.getObject.isLiteral) y.getObject).distinct()
//    x.foreach(println(_))

    val sObjectProperty = sourceOntology.filter(q => q.getObject.isURI && q.getObject.getLocalName == "ObjectProperty").distinct()
    println("Number of object properties is "+sObjectProperty.count())
    sObjectProperty.foreach(println(_))

    val sAnnotationProperty = sourceOntology.filter(q => q.getObject.isURI && q.getObject.getLocalName == "AnnotationProperty").distinct()
    println("Number of annotation properties is "+sAnnotationProperty.count())
    sAnnotationProperty.foreach(println(_))

    val sDatatypeProperty = sourceOntology.filter(q => q.getObject.isURI && q.getObject.getLocalName == "DatatypeProperty").distinct()
    println("Number of Datatype properties is "+sDatatypeProperty.count())
    sDatatypeProperty.foreach(println(_))

    val sClass = sourceOntology.filter(q => q.getSubject.isURI && q.getObject.isURI && q.getObject.getLocalName == "Class").distinct()
    println("Number of classes is "+sClass.count())
    sClass.foreach(println(_))

    //################### SparkNLP #############################
//    val pipeline = PretrainedPipeline("spell_check_ml","en")
//    val result: Map[String, Seq[String]] = pipeline.annotate("Harry Potter is a great movie")
//    println("The output is "+result("spell"))
//
//    val explainDocumentPipeline = PretrainedPipeline("explain_document_ml")
//    val annotations: Map[String, Seq[String]] = explainDocumentPipeline.annotate("We are very happy about SparkNLP")
//    println("The output is "+annotations)
//    println("The END")
//    sparkSession1.stop()
  }
}
