# MULON - software framework for MULtilingual Ontology mergiNg
Create a multilingual ontology by merging two ontologies from different natural languages

All implementations are based on Scala 2.11.11 and Apache Spark 2.3.1. 

How to use
----------
````
git clone https://github.com/shmkhaled/MULON.git
cd MULON

mvn clean package
````
Input
----------
We use [SANSA](https://github.com/SANSA-Stack) readers to build a dataset of RDD[graph.Triple] for input ontologies.
````
val O1 = ".../firstOntology-de.nt"
val O2 = ".../secondOntology-en.nt"

val offlineDictionaryForO1 = ".../firstOntology-de.csv
val offlineDictionaryForO2 = ".../secondOntology-en.csv
 
val lang = Lang.NTRIPLES
val O1triples = spark.rdf(lang)(O1)
val O2triples = spark.rdf(lang)(O2)
````

Example
----------

````
val multilingualMergedOntology = ontoMerge.MergeOld(O1triples, O2triples, offlineDictionaryForO1, offlineDictionaryForO2)
 
val ontStat = new OntologyStatistics(sparkSession1)
println("Statistics for merged ontology")
ontStat.GetStatistics(multilingualMergedOntology)
     
//Assessemnt sheet
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

````

The subsequent steps depend on your IDE. Generally, just import this repository as a Maven project and start using MULON.
