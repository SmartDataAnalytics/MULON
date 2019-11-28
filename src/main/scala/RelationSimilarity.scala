import org.apache.spark.rdd.RDD

/*
* Created by Shimaa Ibrahim 4 November 2019
* */ class RelationSimilarity {

  def GetRelationSimilarity(listOfRelationsInTargetOntology: RDD[(String)], listOfRelationsInSourceOntology: RDD[(String, String, String)]) = {
    var crossRelations: RDD[(String, (String, String, String))] = listOfRelationsInTargetOntology.cartesian(listOfRelationsInSourceOntology)
    val gS = new GetSimilarity()
    val p = new PreProcessing()
    var sim: RDD[(String, String, String, String, Double)] = crossRelations.map(x => (x._2._1, x._2._2, x._2._3, x._1, gS.getSimilarity(p.removeStopWordsFromEnglish(p.splitCamelCase(x._1).toLowerCase), p.removeStopWordsFromEnglish(p.splitCamelCase(x._2._3).toLowerCase)))).filter(y => y._5 > 0.8)
    sim
  }

}
