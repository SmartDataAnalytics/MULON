# MULON
MULON (MULtilingual Ontology mergiNg) is a software framework for create a multilingual ontology by merging two ontologies from different natural languages.

All implementations are based on Scala 2.11.11 and Apache Spark 2.3.1. 

How to use
----------
````
git clone https://github.com/SmartDataAnalytics/MULON.git
cd MULON

mvn clean package
````
Input
----------
MULON reads input ontologies in Turtle and uses [SANSA](https://github.com/SANSA-Stack) readers to build a dataset of RDD[graph.Triple] for each ontology.
````
val O1 = ".../firstOntology-de.ttl"
val O2 = ".../secondOntology-en.ttl"
````

Documentation
----------
A brief description of MULON can be found [here](https://smartdataanalytics.github.io/MULON/). 
Furthermore, a description for each configurable parameter and function can be found [here](https://smartdataanalytics.github.io/MULON/DocumentationIndex.html#package).



The subsequent steps depend on your IDE. Generally, just import this repository as a Maven project and start using MULON.
