name := "YouthTobaccoSurvey"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.11.8"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.0.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-tags_2.11
libraryDependencies += "org.apache.spark" % "spark-tags_2.11" % "2.0.0"
