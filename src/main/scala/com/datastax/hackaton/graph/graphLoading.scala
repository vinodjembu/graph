package com.datastax.hackaton.graph

import com.datastax.bdp.graph.spark.graphframe._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ StructField, StructType }
import org.apache.spark.sql.types._

object graphLoading {
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

    val categorySchema: StructType = {
      StructType(Seq(
        StructField("category_id", IntegerType, true),
        StructField("category", StringType, true)))
    }

    recipe = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferSchema", "true").load("file:///Users//vinodjembu//Documents//DSEGraphHackaton//RecipeData//Recipe//recipes.csv")
    categories = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(categorySchema).load("file:///Users//vinodjembu//Documents//DSEGraphHackaton//RecipeData//Recipe//category.csv")

    receipeCategories = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferSchema", "true").load("file:///Users//vinodjembu//Documents//DSEGraphHackaton//RecipeData//Recipe//receipecategories.csv")

    // Write out vertices
    println("\nWriting recipes vertices")
    g.updateVertices(recipe.withColumn("~label", lit("recipe")))

    println("\nWriting category vertices")
    g.updateVertices(categories.withColumn("~label", lit("categories")))

    // Write out edges
    println("\nWriting receipe-Category edges")
    val receipeCategoriesEdges = receipeCategories.withColumn("srcLabel", lit("recipe")).withColumn("dstLabel", lit("categories")).withColumn("edgeLabel", lit("belongsTo"))
    g.updateEdges(receipeCategoriesEdges.select(g.idColumn(col("srcLabel"), col("recipe_id")) as "src", g.idColumn(col("dstLabel"), col("category_id")) as "dst", col("edgeLabel") as "~label"))

    System.exit(0)
  }
}