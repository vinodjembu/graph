# Graph-Loading - DSE Graph - Spark Graphframex

## Maven Build
mvn clean install

## Build Graph Schema
Gremlin commands to create schema added under /graph-schema folder

 
![alt text](https://github.com/vinodjembu/graph/blob/master/images/Screen%20Shot%202019-08-01%20at%202.29.18%20PM.png "Schema")


## Data Ingestion - Spark
dse spark-submit --class com.datastax.hackaton.graph.graphLoading /target/graph-0.0.1-SNAPSHOT.jar


## Data Ingestion - Spark (Multi-Threaded)
dse spark-submit --class com.datastax.hackaton.graph.graphLoadingWithMultiThread /target/graph-0.0.1-SNAPSHOT.jar

## Output

![alt text](https://github.com/vinodjembu/graph/blob/master/images/Screen%20Shot%202019-08-01%20at%202.47.17%20PM.png "Graph Output")
