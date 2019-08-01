package com.datastax.hackaton.graph

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType

import com.datastax.bdp.graph.spark.graphframe.toSparkSessionFunctions
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * Data Loading through Spark GraphFrameX - Multi-Threaded
 * @ vinod-Jembu (Datastax)
 */
object graphLoadingWithMultiThread {
  def main(args: Array[String]): Unit = {
    //Graph Name
    val graphName = "foodreceipeThread"

    val spark = SparkSession
      .builder
      .appName("Data Import Application")
      .enableHiveSupport()
      .getOrCreate()

    val g = spark.dseGraph(graphName)

    //DataFrame
    var recipe: DataFrame = null
    var categories: DataFrame = null
    var receipeCategories: DataFrame = null
    var incredient: DataFrame = null
    var receipeIncredients: DataFrame = null

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

    val categorySchema: StructType = {
      StructType(Seq(
        StructField("category_id", IntegerType, true),
        StructField("category", StringType, true)))
    }

    val receipeIncredientsSchema: StructType = {
      StructType(Seq(
        StructField("recipe_id", StringType, true),
        StructField("incredient_name", StringType, true),
        StructField("comment", StringType, true),
        StructField("qty", StringType, true),
        StructField("unit", StringType, true)))
    }

    val start = Instant.now()
    println("\nStart Time" + start)

    //Vertices
    recipe = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferSchema", "true").load("file:///Users//vinodjembu//Documents//DSEGraphHackaton//RecipeData//Recipe//recipes.csv")
    categories = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(categorySchema).load("file:///Users//vinodjembu//Documents//DSEGraphHackaton//RecipeData//Recipe//category.csv")
    incredient = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferSchema", "true").load("file:///Users//vinodjembu//Documents//DSEGraphHackaton//RecipeData//Recipe//incredients.csv")

    //EDGES
    receipeIncredients = spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(receipeIncredientsSchema).load("file:///Users//vinodjembu//Documents//DSEGraphHackaton//RecipeData//Recipe//incredient_receipe.csv")
    receipeCategories = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferSchema", "true").load("file:///Users//vinodjembu//Documents//DSEGraphHackaton//RecipeData//Recipe//receipecategories.csv")

    //Build Vertices Map
    val vertices = Map("recipe" -> recipe, "categories" -> categories, "incredient" -> incredient)

    //Iterate Each Map create as Separate Thread
    vertices.foreach { a: (String, DataFrame) =>
      val key = a._1
      val value = a._2

      var verticeThread = new UpdateVerticesThread(spark, value, key)
      verticeThread.start()
    }

    // Write out edges
    println("\nWriting receipe-Category edges")
    val receipeCategoriesEdges = receipeCategories.withColumn("srcLabel", lit("recipe")).withColumn("dstLabel", lit("categories")).withColumn("edgeLabel", lit("avaialbleBy"))
    g.updateEdges(receipeCategoriesEdges.select(g.idColumn(col("srcLabel"), col("recipe_id")) as "src", g.idColumn(col("dstLabel"), col("category_id")) as "dst", col("edgeLabel") as "~label"))

    println("\nWriting receipe-incredient edges")
    val receipeIncredientsEdges = receipeIncredients.withColumn("srcLabel", lit("recipe")).withColumn("dstLabel", lit("incredient")).withColumn("edgeLabel", lit("contains"))
    g.updateEdges(receipeIncredientsEdges.select(g.idColumn(col("srcLabel"), col("recipe_id")) as "src", g.idColumn(col("dstLabel"), col("incredient_name")) as "dst", col("edgeLabel") as "~label", col("comment") as "comment", col("qty") as "qty", col("unit") as "unit"))

    val end = Instant.now()
    println("Ending Time" + end)
    
    val diffInSecs = ChronoUnit.SECONDS.between(start, end)
    println("\n Total Time taken " + diffInSecs)

    System.exit(0)
  }
}