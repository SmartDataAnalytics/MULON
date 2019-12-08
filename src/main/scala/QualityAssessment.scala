import org.apache.jena.graph
import org.apache.spark.rdd.RDD
import net.sansa_stack.rdf.spark.qualityassessment._
import org.apache.jena.graph.Triple
import org.apache.spark.sql.SparkSession
/*
* Created by Shimaa Ibrahim 28 October 2019
* */
class QualityAssessment(sparkSession: SparkSession) {
  val ontoStat = new OntologyStatistics(sparkSession)

  def SchemaCompleteness(ontologyTriples: RDD[graph.Triple]): Double={
    val schemaComp: Double = ontologyTriples.assessSchemaCompleteness()
    schemaComp
  }
  def InterlinkingCompleteness(ontologyTriples: RDD[graph.Triple]): Double={
    val interlinkingComp: Double = ontologyTriples.assessInterlinkingCompleteness()
    interlinkingComp
  }
  def DereferenceableUris(ontologyTriples: RDD[graph.Triple]): Double={
    val dereferenceableUris: Double = ontologyTriples.assessDereferenceableUris()
    dereferenceableUris
  }
  def CoverageDetail(ontologyTriples: RDD[graph.Triple]): Double={
    val coverageDetail: Double = ontologyTriples.assessCoverageDetail()
    coverageDetail
  }
  def AmountOfTriples(ontologyTriples: RDD[graph.Triple]): Double={
    val amountOfTriples: Double = ontologyTriples.assessAmountOfTriples()
    amountOfTriples
  }
  def NoHashUris(ontologyTriples: RDD[graph.Triple]): Double={
    val noHashUris: Double = ontologyTriples.assessNoHashUris()
    noHashUris
  }
  def LabeledResources(ontologyTriples: RDD[graph.Triple]): Double={
    val labeledResources: Double = ontologyTriples.assessLabeledResources()
    labeledResources
  }
  def assessSchemaCompleteness(dataset: RDD[Triple]): Double = {
    /**
      * -->Rule->Filter-->
      * select (?p2, o) where ?s p=rdf:type isIRI(?o); ?p2 ?o2
      * -->Action-->
      * S+=?p2 && SC+=?o
      * -->Post-processing-->
      * |S| intersect |SC| / |SC|
      */

    val p2_o = dataset.filter(f =>
      f.getPredicate.getLocalName.equals("type")
        && f.getObject.isURI()).cache()
    println("number of classes and relations = "+p2_o.count())
    p2_o.foreach(println(_))

    val S = p2_o.map(_.getObject).distinct() //was _.getPredicate
    println("S = "+S.count())
    S.foreach(println(_))
    val SC = dataset.map(_.getObject).distinct()
    println("SC = "+SC.count())
    SC.foreach(println(_))

    val S_intersection_SC = S.intersection(SC).distinct
    println("S_intersection_SC "+S_intersection_SC.count())
    S_intersection_SC.foreach(println(_))

    val SC_count = SC.count()
    val S_intersection_SC_count = S_intersection_SC.count()

    if (SC_count > 0) S_intersection_SC_count.toDouble / SC_count.toDouble
    else 0.00
  }

  def AttributeRichness(ontologyTriples: RDD[graph.Triple]): Double={
    /*refers to how much knowledge about classes is inthe schema. The more attributes are defined, the more knowledge the ontol-ogy provides. = the number of attributes for all classes divided by the number of classes (C).*/
    val numOfRelations = ontoStat.GetNumberOfRelations(ontologyTriples)
//    println("Number of Relations = "+numOfRelations)
    val numOfClasses = ontoStat.GetNumberOfClasses(ontologyTriples)
//    println("Number of Classes = "+numOfClasses)
    val attributeRichness: Double = numOfRelations / numOfClasses
    ontoStat.Round(attributeRichness)
  }

  def RelationshipRichness(ontologyTriples: RDD[graph.Triple]): Double={
    /*refers to the diversity of relations and their position in the ontology. The more relations the ontology has (except \texttt{rdfs:subClassOf} relation), the richer it is.= number of object property / (subClassOf + object property)*/
    val numOfRelations = ontoStat.GetNumberOfRelations(ontologyTriples)
    val numOfSubClassOf = ontoStat.GetNumberOfSubClasses(ontologyTriples)
    val relationshipRichness = numOfRelations / (numOfSubClassOf + numOfRelations)
    ontoStat.Round(relationshipRichness)
  }

  def InheritanceRichness(ontologyTriples: RDD[graph.Triple]): Double={
    /*refers to how well knowledge is distributed across different  levels  in  the  ontology. = the number of sub-classes divided by the sum of the number of classes. */
    val numOfSubClassOf = ontoStat.GetNumberOfSubClasses(ontologyTriples)
    val numOfClasses = ontoStat.GetNumberOfClasses(ontologyTriples)
    ontoStat.Round(numOfSubClassOf/numOfClasses)
  }

  def Readability(ontologyTriples: RDD[graph.Triple]): Double={
    /*refers to the the existence of human readable descriptions(HRD) in the ontology, such as comments, labels, or description. The morehuman readable descriptions exist, the more readable the ontology is. HRD / number of triples*/
    val numOfHRD = ontoStat.GetNumberOfHRD(ontologyTriples)
    val numOfTriples = ontologyTriples.distinct().count()
    ontoStat.Round(numOfHRD/numOfTriples)
  }
//  def EnrichmentStatistics(targetOntology: RDD[graph.Triple], enrichedTargetOntology: RDD[graph.Triple])={
//    val numOfClassesBeforeEnrichment = ontoStat.GetNumberOfClasses(targetOntology)
//    val numOfClassesAfterEnrichment = ontoStat.GetNumberOfClasses(enrichedTargetOntology)
//    val enrichmentRatioForClasses = ((numOfClassesAfterEnrichment-numOfClassesBeforeEnrichment)/numOfClassesBeforeEnrichment)*100
//    println("Number of classes before enrichment = "+ numOfClassesBeforeEnrichment+" after enrichment = "+numOfClassesAfterEnrichment+ " i.e. enrichment by "+ enrichmentRatioForClasses.round + " %")
//
//    val numOfRelationsBeforeEnrichment = ontoStat.GetNumberOfRelations(targetOntology)
//    val numOfRelationsAfterEnrichment = ontoStat.GetNumberOfRelations(enrichedTargetOntology)
//    val enrichmentRatioForRelations = ((numOfRelationsAfterEnrichment-numOfRelationsBeforeEnrichment)/numOfRelationsBeforeEnrichment)*100
//    println("Number of relations before enrichment = "+numOfRelationsBeforeEnrichment+" after enrichment = "+numOfRelationsAfterEnrichment+ " i.e. enrichment by "+ enrichmentRatioForRelations.round + " %")
//
//    val numOfTriplesBeforeEnrichment = targetOntology.count()
//    val numOfTriplesAfterEnrichment = enrichedTargetOntology.count()
//    val triplesRatio = ((numOfTriplesAfterEnrichment-numOfTriplesBeforeEnrichment)/numOfTriplesBeforeEnrichment)*100
//    println("Number of triples before enrichment = "+numOfTriplesBeforeEnrichment+" after enrichment = "+numOfTriplesAfterEnrichment+ " i.e. increased by "+ triplesRatio.round + " %")
//
//    val relationshipRichnessBeforeEnrichment = this.RelationshipRichness(targetOntology)
//    val relationshipRichnessAfterEnrichment = this.RelationshipRichness(enrichedTargetOntology)
//    val enrichmentRatioForRelationshipRichness = ((relationshipRichnessAfterEnrichment-relationshipRichnessBeforeEnrichment)/relationshipRichnessBeforeEnrichment)*100
//    println("Relationship richness before enrichment = "+ontoStat.Round(relationshipRichnessBeforeEnrichment)+" after enrichment = "+ontoStat.Round(relationshipRichnessAfterEnrichment)+ " i.e. increased by "+ ontoStat.Round(enrichmentRatioForRelationshipRichness)+ " %")
//
//    val inheritanceRichnessBeforeEnrichment = this.InheritanceRichness(targetOntology)
//    val inheritanceRichnessAfterEnrichment = this.InheritanceRichness(enrichedTargetOntology)
//    val inheritanceRichnessRation = ((inheritanceRichnessAfterEnrichment-inheritanceRichnessBeforeEnrichment)/inheritanceRichnessBeforeEnrichment)*100
//    println("Inheritance richness before enrichment = "+ontoStat.Round(inheritanceRichnessBeforeEnrichment)+" after enrichment = "+ontoStat.Round(inheritanceRichnessAfterEnrichment)+ " i.e. increased by "+ ontoStat.Round(inheritanceRichnessRation)+ " %")
//
//
//  }
}
