import org.apache.jena.graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*
* Created by Shimaa Ibrahim 28 October 2019
* */ class QualityAssessment(sparkSession: SparkSession) {
  val ontoStat = new OntologyStatistics(sparkSession)

  def AttributeRichness(ontologyTriples: RDD[graph.Triple]): Double = {
    /*refers to how much knowledge about classes is inthe schema. The more attributes are defined, the more knowledge the ontol-ogy provides. = the number of attributes for all classes divided by the number of classes (C).*/ val numOfRelations = ontoStat.GetNumberOfRelations(ontologyTriples)
    //    println("Number of Relations = "+numOfRelations)
    val numOfClasses = ontoStat.GetNumberOfClasses(ontologyTriples)
    //    println("Number of Classes = "+numOfClasses)
    val attributeRichness: Double = numOfRelations / numOfClasses
    ontoStat.Round(attributeRichness)
  }

  def RelationshipRichness(ontologyTriples: RDD[graph.Triple]): Double = {
    /*refers to the diversity of relations and their position in the ontology. The more relations the ontology has (except \texttt{rdfs:subClassOf} relation), the richer it is.= number of object property / (subClassOf + object property)*/ val numOfRelations = ontoStat.GetNumberOfRelations(ontologyTriples)
    val numOfSubClassOf = ontoStat.GetNumberOfSubClasses(ontologyTriples)
    val relationshipRichness = numOfRelations / (numOfSubClassOf + numOfRelations)
    ontoStat.Round(relationshipRichness)
  }

  def InheritanceRichness(ontologyTriples: RDD[graph.Triple]): Double = {
    /*refers to how well knowledge is distributed across different  levels  in  the  ontology. = the number of sub-classes divided by the sum of the number of classes. */ val numOfSubClassOf = ontoStat.GetNumberOfSubClasses(ontologyTriples)
    val numOfClasses = ontoStat.GetNumberOfClasses(ontologyTriples)
    ontoStat.Round(numOfSubClassOf / numOfClasses)
  }

  def Readability(ontologyTriples: RDD[graph.Triple]): Double = {
    /*refers to the the existence of human readable descriptions(HRD) in the ontology, such as comments, labels, or description. The morehuman readable descriptions exist, the more readable the ontology is. HRD / number of resources*/ val numOfHRD = ontoStat.GetNumberOfHRD(ontologyTriples)
//    val numOfTriples = ontologyTriples.distinct().count()
    val numOfResources = ontoStat.GetAllResources(ontologyTriples).count().toDouble
    ontoStat.Round(numOfHRD / numOfResources)
  }

  def IsolatedElements(ontologyTriples: RDD[graph.Triple]): Double = {
    /*refers to classes and properties which are defined but not connected to the rest of the ontology, i.e. not used. = (isolated classes + isolated properties)/(classes + properties) */ val numOfIsolatedElements = ontoStat.ResourceDistribution(ontologyTriples).filter(x => x == 1).count()
    val numOfClasses = ontoStat.GetNumberOfClasses(ontologyTriples)
    val numOfProperties = ontoStat.GetNumberOfRelations(ontologyTriples) + ontoStat.GetNumberOfAttributes(ontologyTriples)
    ontoStat.Round(numOfIsolatedElements / (numOfClasses + numOfProperties))
  }

  def ClassCoverage(O1: RDD[graph.Triple], O2: RDD[graph.Triple], Om: RDD[graph.Triple], numberOfMatchedClasses: Int): Double = {
    /* refers  to  how  many  classes  in  the  input  ontologies C1+C2 are preserved in the merged ontology Cm excluding matched classes Cmatch. */ val numOfMergedClasses = ontoStat.GetNumberOfClasses(Om)
    val numOfClassesO1 = ontoStat.GetNumberOfClasses(O1)
    val numOfClassesO2 = ontoStat.GetNumberOfClasses(O2)
    ontoStat.Round(numOfMergedClasses / (numOfClassesO1 + numOfClassesO2 - numberOfMatchedClasses))
  }

  def PropertyCoverage(O1: RDD[graph.Triple], O2: RDD[graph.Triple], Om: RDD[graph.Triple], numberOfMatchedProperties: Int): Double = {
    val numOfMergedProperties = ontoStat.GetAllProperties(Om).count().toDouble
    val numOfPropertiesO1 = ontoStat.GetAllProperties(O1).count().toDouble
    val numOfPropertiesO2 = ontoStat.GetAllProperties(O2).count().toDouble
    ontoStat.Round(numOfMergedProperties / (numOfPropertiesO1 + numOfPropertiesO2 - numberOfMatchedProperties))
  }

  def Compactness(O1: RDD[graph.Triple], O2: RDD[graph.Triple], Om: RDD[graph.Triple]): Double = {
    /* refers  to  how  much  the  size  of  the  merged  ontology compared to the input ontologies. The smaller size of merged ontology, themore the ontology is compacted, e.g. if some resources are removed in order to avoid redundant resources in the merged ontology.*/ ontoStat.Round(ontoStat.GetAllResources(Om).count().toDouble / (ontoStat.GetAllResources(O1).count().toDouble + ontoStat.GetAllResources(O2).count().toDouble))

  }
}
