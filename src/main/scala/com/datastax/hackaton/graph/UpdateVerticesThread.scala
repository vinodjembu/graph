package com.datastax.hackaton.graph

import com.datastax.bdp.graph.spark.graphframe._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * Vertices Thread 
 * Pass SparkSession, Dataframe (Vertice), Vertice label
 * @ Vinod-Jembu (datastax)
 */
class UpdateVerticesThread( spark:SparkSession,v: DataFrame , label:String) extends Thread {

  override def run() {
    
    val graphName = "foodreceipeThread"
    val g = spark.dseGraph(graphName)
    
     println("\nWriting "+label+" vertices")
    // Running Thread, Vertices Name
     g.updateVertices(v.withColumn("~label", lit(label)))

  }
}  
  
