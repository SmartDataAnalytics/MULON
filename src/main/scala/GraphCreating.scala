import org.apache.jena.graph
import org.apache.jena.graph.NodeFactory
import org.apache.spark.rdd.RDD

class GraphCreating{

  def CreateMultilingualEnglishLabels(resourcesWithURIs: RDD[(String, String, String)]): RDD[graph.Triple]={
    val g = resourcesWithURIs.map{case x => graph.Triple.create(
      NodeFactory.createURI(x._1),
      NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#label"),
      NodeFactory.createLiteral(x._3.toLowerCase.capitalize, "en")
    )}
    g
  }
  def CreateMultilingualGermanLabels(resourcesWithURIs: RDD[(String, String, String)]): RDD[graph.Triple]={
    val g = resourcesWithURIs.map{case x => graph.Triple.create(
      NodeFactory.createURI(x._1),
      NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#label"),
      NodeFactory.createLiteral(x._3.toLowerCase.capitalize, "de")
    )}
    g
  }
  def CreateGraph(ontology: RDD[(String, String, String)]): RDD[graph.Triple]={
    val g = ontology.map{case x =>
      if (x._2 == "type" && !x._3.contains("http")){
        //println("case 1")
        graph.Triple.create(
        NodeFactory.createURI(x._1),
        NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
        NodeFactory.createURI("http://www.w3.org/2002/07/owl#"++x._3))}
      else if ((x._2 == "domain" || x._2 == "subPropertyOf" || x._2 == "subClassOf") && x._3 != "Thing"){
        //println("case 2")
        graph.Triple.create(
          NodeFactory.createURI(x._1),
          NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#"++x._2),
          NodeFactory.createURI(x._3)
        )}
      else if (x._2 == "range" && !x._3.contains("http")){
        //println("case 3")
        graph.Triple.create(
          NodeFactory.createURI(x._1),
          NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#"++x._2),
          NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#"++x._3)
        )}
      else if (x._2 == "range" && x._3.contains("http")){
        //println("case 3")
        graph.Triple.create(
          NodeFactory.createURI(x._1),
          NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#"++x._2),
          NodeFactory.createURI(x._3)
        )}
      else if (x._2 == "inverseOf" || x._2 == "disjointWith"){
        //println("case 4")
        graph.Triple.create(
          NodeFactory.createURI(x._1),
          NodeFactory.createURI("http://www.w3.org/2002/07/owl#"++x._2),
          NodeFactory.createURI(x._3)
        )}
      else if (x._2 == "subClassOf" && x._3 == "Thing"){
        //println("case 4")
        graph.Triple.create(
          NodeFactory.createURI(x._1),
          NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#"++x._2),
          NodeFactory.createURI("http://www.w3.org/2002/07/owl#"++x._3)
        )}
      else {//println("case 5")
        graph.Triple.create(
        NodeFactory.createURI(x._1),
        NodeFactory.createURI(x._2),
        NodeFactory.createURI(x._3)
      )}}
    g
  }

}
