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
val O1 = ".../firstOntology-en.nt"
val O2 = ".../secondOntology-de.nt"
val offlineDictionaryForO1 = ".../firstOntology-en.csv
val offlineDictionaryForO2 = ".../secondOntology-de.csv
 
val lang = Lang.NTRIPLES
val O1triples = spark.rdf(lang)(O1)
val O2triples = spark.rdf(lang)(O2)
````

Example
----------

````
//Resources Extraction
val O1Classes = ontStat.GetAllClasses(O1triples)
val O2Classes = ontStat.GetAllClasses(O2triples)

val availableTranslations = translate.GettingAllAvailableTranslations(offlineDictionaryForO2)

````

The subsequent steps depend on your IDE. Generally, just import this repository as a Maven project and start using MULON.
