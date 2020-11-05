package Eksperimen

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkSQL {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local[*]")
      .appName("SparkSQL")
      .getOrCreate()

    val schemaTable = StructType(
      List(
        StructField("age", IntegerType, true),
        StructField("workclass", StringType, true),
        StructField("zip", StringType, true),
        StructField("education", StringType, true),
        StructField("year_of_education", IntegerType, true),
        StructField("marital_status", StringType, true),
        StructField("occupation", StringType, true),
        StructField("relationship", StringType, true),
        StructField("race", StringType, true),
        StructField("sex", StringType, true),
        StructField("capital_gain", IntegerType, true),
        StructField("capital_loss", IntegerType, true),
        StructField("hours_per_week", IntegerType, true),
        StructField("native_country", StringType, true),
        StructField("income", StringType, true)
      )
    )


    val peopleDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .schema(schemaTable)
      .load("input/adult100k.csv")

    peopleDF.printSchema()

    // Query
    peopleDF.createTempView("tAdults")
    val query = spark.sql("SELECT workclass, count(education) as count_people " +
                                    "FROM tAdults " +
                                    "WHERE lower(education) == 'bachelors'" +
                                    "GROUP BY workclass " +
                                    "ORDER BY count_people DESC " +
                                    "LIMIT 10")

//    query.write.csv("C:/Users/asus/Desktop/resultquery1.csv")


    // Statistika: count, mean, stddev, min, max
    peopleDF.describe().show()

//    // Median
//    val median = spark.sql("SELECT percentile_approx(age, 0.5) as Median " +
//                                    "FROM tAdults").show()
//
//    // Modus
//    val modus = spark.sql( "SELECT age as Modus " +
//                                    "FROM tAdults " +
//                                    "GROUP BY age " +
//                                    "ORDER BY COUNT(age) DESC " +
//                                    "LIMIT 1").show()



  }
}
