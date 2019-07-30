package com.datastax.hackaton.graph

import org.apache.spark.sql.types._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql._
import com.datastax.spark.connector.streaming._
import org.apache.spark.sql.streaming.OutputMode

import com.datastax.bdp.graph.spark.graphframe._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ StructField, StructType }
 import org.apache.spark.sql.{ForeachWriter, Row}

object graphStreamLoad {
  def main(args: Array[String]): Unit = {

    val graphName = "foodreceipe"

    val spark = SparkSession
      .builder
      .appName("Data Import Application")
      .enableHiveSupport()
      .getOrCreate()

    val g = spark.dseGraph(graphName)
    var recipe: DataFrame = null
    var categories: DataFrame = null

    var receipeCategories: DataFrame = null

    // Create Schemas for DataSets where explicit types are necessary
    // (Sometimes inferring the schema doesn't yield the correct type)
    val recipesSchema: StructType = {
      StructType(Seq(
        StructField("recipe_id", IntegerType, true),
        StructField("title", StringType, true),
        StructField("sodium", DoubleType, true),
        StructField("date", TimestampType, true),
        StructField("calories", DoubleType, true),
        StructField("descr", StringType, true),
        StructField("protein", StringType, true),
        StructField("fat", StringType, true),
        StructField("rating", DoubleType, true)))
    }

    recipe = spark.readStream.option("header", "true").schema(recipesSchema).json("file:///Users//vinodjembu//Documents//DSEGraphHackaton//loading")
   /* recipe.writeStream.foreach(
      new ForeachWriter[String] {

        def open(partitionId: Long, version: Long): Boolean = {
          // Open connection
        }

        def process(record: String) = {
          // Write string to connection
        }

        def close(errorOrNull: Throwable): Unit = {
          // Close the connection
        }
      }).start()
 */
    // Write out vertices
    println("\nWriting recipes vertices")

    //Writing to Cassandra
    recipe.writeStream.option("checkpointLocation", "file:///Users/vinodjembu/Documents/loadFiles/chpt-1/").format("com.datastax.bdp.graph.spark.graphframe.DseGraphFrame")

  }
}