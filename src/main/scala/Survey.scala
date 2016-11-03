package com.cnadig.youth.tobacco.survey
import org.apache.spark.sql.SparkSession
object Survey{

  def main(args : Array[String]) = {
    println("HelloWorld")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .getOrCreate()


    val tobacco_schema_path = "/home/cnadig/Developer/CodingExercise/tobacco_schema.json"
    val tobaccoFieldsArray = spark.read.json(tobacco_schema_path).select(explode($"meta.view.columns").as("columns")).select("columns.name").map(x => x.getString(0)).collect()


    val tobacco_data_path = "/home/cnadig/Developer/CodingExercise/tobacco_data.json"
    val tobaccoData = spark.read.json(tobacco_data_path).select("data")
    val elements = tobaccoData.select($"data").first.getList(0).size
    val tobaccoDataDF = tobaccoData.select((Range(0, elements).map(idx => $"data"(idx) as tobaccoFieldsArray(idx) ):_*))





  }

}