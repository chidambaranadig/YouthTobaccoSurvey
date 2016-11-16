package com.cnadig.youth.tobacco.survey

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import org.apache.spark.sql.functions._

object Survey{

  def main(args : Array[String]) = {


    val spark = SparkSession
                  .builder()
                  .appName("Spark SQL basic example")
                  .enableHiveSupport()
                  .getOrCreate()

    import spark.implicits._

    val tobacco_schema_path = "/home/cnadig/Developer/CodingExercise/tobacco_schema.json"
    val tobaccoSchema = spark.read.json(tobacco_schema_path)

    val tobaccoDataColumns = tobaccoSchema.select($"meta.view.columns.name").first
                                          .mkString(",")
                                          .replaceFirst("WrappedArray\\(","")
                                          .replaceAll("\\)","")
                                          .replaceAll("\\s","")
                                          .split(",")


    val tobacco_data_path = "/home/cnadig/Developer/CodingExercise/tobacco_data.json"
    val tobaccoData = spark.read.json(tobacco_data_path).select("data")
    val tobaccoDataDF = tobaccoData.select(
            $"data"(0) as "sid",
            $"data"(1) as "id",
            $"data"(2) as "position",
            $"data"(3) as "created_at",
            $"data"(4) as "created_meta",
            $"data"(5) as "updated_at",
            $"data"(6) as "updated_meta",
            $"data"(7) as "meta",
            $"data"(8) as "YEAR",
            $"data"(9) as "LocationAbbr",
            $"data"(10) as "LocationDesc",
            $"data"(11) as "TopicType",
            $"data"(12) as "TopicDesc",
            $"data"(13) as "MeasureDesc",
            $"data"(14) as "DataSource",
            $"data"(15) as "Response",
            $"data"(16) as "Data_Value_Unit",
            $"data"(17) as "Data_Value_Type",
            $"data"(18) as "Data_Value",
            $"data"(19) as "Data_Value_Footnote_Symbol",
            $"data"(20) as "Data_Value_Footnote",
            $"data"(21) as "Data_Value_Std_Err",
            $"data"(22) as "Low_Confidence_Limit",
            $"data"(23) as "High_Confidence_Limit",
            $"data"(24) as "Sample_Size",
            $"data"(25) as "Gender",
            $"data"(26) as "Race",
            $"data"(27) as "Age",
            $"data"(28) as "Education",
            $"data"(29) as "GeoLocation",
            $"data"(30) as "TopicTypeId",
            $"data"(31) as "TopicId",
            $"data"(32) as "MeasureId",
            $"data"(33) as "StratificationID1",
            $"data"(34) as "StratificationID2",
            $"data"(35) as "StratificationID3",
            $"data"(36) as "StratificationID4",
            $"data"(37) as "SubMeasureID",
            $"data"(38) as "DisplayOrder"
    )

    val topicDimension = tobaccoDataDF.select($"TopicType",$"TopicDesc").distinct.filter(x => x.getString(0) != null)
    val topicDimensionWithId = topicDimension.select( concat($"TopicType", lit(","), $"TopicDesc") as "Topic Dimension").withColumn("TopicId",monotonically_increasing_id)

    topicDimensionWithId.write.mode("verwrite").save("/home/cnadig/Developer/CodingExercise/TopicDimension.parquet")ppppppppppp47
    \\\

  }
}