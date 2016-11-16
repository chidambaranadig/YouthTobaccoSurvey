#Youth Tobacco Survey

Spark Application that creates a Star Schema Design on the Youth Tobacco Survey Data.

### Spark Version
This application is coded to run on Spark 2.0 with Scala 2.11.

### Implementation
- The Spark Jar is built using SBT.

- The Spark Job can be started using the spark-submit command.
 
- The Spark Application reads a corpus of Youth Tobacco Survey data and its Schema in JSON format.

- The Data is then separated into Facts and Dimensions.

- The Facts and Dimension tables are then stored into separate parquet data stores.