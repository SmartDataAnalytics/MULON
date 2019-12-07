import org.apache.jena.graph
import org.apache.jena.graph.Node
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
/*
* Created by Shimaa 14.oct.2019
* */
class OntologyRebuilding (sparkSession: SparkSession) {
  def RebuildOntologyWithLabels(ontologyTriples: RDD[graph.Triple]): RDD[(String, String, String)] = {
    val ontologyLabels: Map[Node, graph.Triple] = ontologyTriples.filter(x=>x.getPredicate.getLocalName == "label").keyBy(_.getSubject).collect().toMap
//    println("All labels ")
//    ontologyLabels.foreach(println(_))
    val labelBroadcasting: Broadcast[Map[Node, graph.Triple]] = sparkSession.sparkContext.broadcast(ontologyLabels)
    val ontoWithLabels = new OntologyWithLabels(labelBroadcasting)
    val Ontology: RDD[(String, String, String)] = ontoWithLabels.RecreateOntologyWithLabels(ontologyTriples)//.cache()
    Ontology
  }
  def RebuildTargetOntologyWithoutCodes(ontologyTriples: RDD[graph.Triple]): RDD[(String, String, String)] = {
    val p = new PreProcessing()
    val onto: RDD[(String, String, String)] = ontologyTriples.filter(y=> y.getPredicate.getLocalName != "label").map(x => if (x.getObject.isURI)(x.getSubject.getLocalName, x.getPredicate.getLocalName, x.getObject.getLocalName)else (x.getSubject.getLocalName, x.getPredicate.getLocalName, x.getObject.toString))

    onto
  }


}
