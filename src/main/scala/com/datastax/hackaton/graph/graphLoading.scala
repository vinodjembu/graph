package com.datastax.hackaton.graph

import com.datastax.bdp.graph.spark.graphframe._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ StructField, StructType }
import org.apache.spark.sql.types._
import java.time.temporal.ChronoUnit
import java.time.Instant

/**
 * Data Loading through Spark GraphFrameX
 * @ vinod-Jembu (Datastax)
 */
object graphLoading {
  def main(args: Array[String]): Unit = {
    val graphName = "foodreceipe"

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
    
    //Vertices ( Loaded through local File-System) - With Schema or InferSchema
    recipe = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferSchema", "true").load("file:///Users//vinodjembu//Documents//DSEGraphHackaton//RecipeData//Recipe//recipes.csv")
    categories = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(categorySchema).load("file:///Users//vinodjembu//Documents//DSEGraphHackaton//RecipeData//Recipe//category.csv")
    incredient = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferSchema", "true").load("file:///Users//vinodjembu//Documents//DSEGraphHackaton//RecipeData//Recipe//incredients.csv")

    //Vertices ( Loaded through DSEFS)
    //recipe = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferSchema", "true").load("dsefs:///tmp//recipes.csv")
    // categories = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(categorySchema).load("dsefs:///tmp//category.csv")
    // incredient = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferSchema", "true").load("dsefs:///tmp//incredients.csv")

    //EDGES ( Loaded through local File-System)
    receipeIncredients = spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(receipeIncredientsSchema).load("file:///Users//vinodjembu//Documents//DSEGraphHackaton//RecipeData//Recipe//incredient_receipe.csv")
    receipeCategories = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferSchema", "true").load("file:///Users//vinodjembu//Documents//DSEGraphHackaton//RecipeData//Recipe//receipecategories.csv")

    //EDGES ( Loaded through DSEFS)
    //receipeIncredients = spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(receipeIncredientsSchema).load("dsefs:///tmp//incredient_receipe.csv")
    //receipeCategories = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferSchema", "true").load("dsefs:///tmp//receipecategories.csv")

    // Write out vertices
    println("\nWriting recipes vertices")
    g.updateVertices(recipe.withColumn("~label", lit("recipe")))

    println("\nWriting category vertices")
    g.updateVertices(categories.withColumn("~label", lit("categories")))

    println("\nWriting incredient vertices")
    g.updateVertices(incredient.withColumn("~label", lit("incredient")))

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