import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.log4j.{Level, Logger}

//from flair.data import Sentence
//from flair.models import SequenceTagger
//from segtok.segmenter import split_single
//from flair.data import segtok_tokenizer

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
//    val sparkSession1 = SparkSession.builder
//      //      .master("spark://172.18.160.16:3090")
//      .master("local[*]")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .getOrCreate()

    println("Hello")
//    val pipeline = PretrainedPipeline("spell_check_ml","en")
//    val result: Map[String, Seq[String]] = pipeline.annotate("Harry Potter is a great movie")
//    println("The output is "+result("spell"))
//
    val explainDocumentPipeline = PretrainedPipeline("explain_document_ml")
    val annotations: Map[String, Seq[String]] = explainDocumentPipeline.annotate("We are very happy about SparkNLP")
    println("The output is "+annotations)
    println("The END")
//    sparkSession1.stop()
  }
}
