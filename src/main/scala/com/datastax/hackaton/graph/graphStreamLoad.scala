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
import org.apache.spark.sql.{ ForeachWriter, Row }
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors


object graphStreamLoad {
  def main(args: Array[String]): Unit = {

    val graphName = "foodreceipe"

    val spark = SparkSession
      .builder
      .appName("Data Import Application")
      .enableHiveSupport()
      .getOrCreate()

    val g = spark.dseGraph(graphName)
    //var recipe: DataFrame = null
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

   val  recipe = spark.readStream.option("header", "true").schema(recipesSchema).json("file:///Users//vinodjembu//Documents//DSEGraphHackaton//loading")
    
   val executor:ExecutorService = Executors.newFixedThreadPool(1)
   
    val df = recipe.toDF();
   /* recipe.writeStream.foreach(
      new ForeachWriter[Row] {

        def open(partitionId: Long, epochId: Long) = {
          val g = spark.dseGraph("foodreceipe")
          true
        }

        def process(row: Row) = {
          val rowAsMap = row.getValuesMap(row.schema.fieldNames)
           println("\nWget values" + rowAsMap)
            println("\n row values" + row)

          //g.updateVertices(recipe.withColumn("~label", lit("recipe")))
        }

        def close(errorOrNull: Throwable): Unit = {
          // Close the connection
        }
      }).start()*/

    // Write out vertices
    println("\nWriting recipes vertices")

    //Writing to Cassandra

  }
}