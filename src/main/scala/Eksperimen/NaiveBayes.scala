package Eksperimen

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object NaiveBayes {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local[*]")
      .appName("NaiveBayes")
      .getOrCreate()

    // Create schema
    val schema = StructType(
      List(
        StructField("age", IntegerType, true),
        StructField("workclass", StringType, true),
        StructField("fnlwgt", IntegerType, true),
        StructField("education", StringType, true),
        StructField("education-num", IntegerType, true),
        StructField("marital-status", StringType, true),
        StructField("occupation", StringType, true),
        StructField("relationship", StringType, true),
        StructField("race", StringType, true),
        StructField("sex", StringType, true),
        StructField("capital-gain", IntegerType, true),
        StructField("capital-loss", IntegerType, true),
        StructField("hours-per-week", IntegerType, true),
        StructField("native-country", StringType, true),
        StructField("salary", StringType, true)
      )
    )

    // Read CSV
    val adult100k_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .schema(schema)
      .load("input/adult100k.csv")


    // Create vector based on stringIndexer and oneHotEncoder
    val cols = adult100k_df.columns
    val encodedFeatures = cols.flatMap{ columnName =>
      val stringIndexer = new StringIndexer()
        .setInputCol(columnName)
        .setOutputCol(columnName + "_Index")
      val oneHotEncoder = new OneHotEncoderEstimator()
        .setInputCols(Array(columnName + "_Index"))
        .setOutputCols(Array(columnName + "_vec"))
        .setDropLast(false)
      Array(stringIndexer.setHandleInvalid("keep"),oneHotEncoder)
    }

    // Pipeline
    val pipeline = new Pipeline().setStages(encodedFeatures)
    val indexer_model = pipeline.fit(adult100k_df)

    // Sparse Vector
    val df_transformed = indexer_model.transform(adult100k_df)

    // Dense Vector
    val sparseToDense = udf((v: Vector) => v.toDense)
    val df_denseVectors = df_transformed.withColumn("dense_workclass_vec",sparseToDense(df_transformed("workclass_vec")))
    df_denseVectors.show(10)

    // Final Result: Feature Vector
    val vecFeatures = df_transformed.columns.filter(_.contains("vec"))
    val vectorAssembler = new VectorAssembler()
      .setInputCols(vecFeatures)
      .setOutputCol("features")
    val pipelineVectorAssembler = new Pipeline().setStages(Array(vectorAssembler))
    val result_df = pipelineVectorAssembler.fit(df_transformed).transform(df_transformed)
    result_df.show(10)

    // Split data into training (60%) and test (40%).
    val Array(training, test) = result_df.randomSplit(Array(0.7, 0.3))
    val model = new NaiveBayes().setModelType("multinomial").setLabelCol("workclass_Index").fit(training)

    val predictions = model.transform(test)
    predictions.select("age","workclass","education","occupation","sex","salary","prediction").show(10)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("workclass_Index")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)

    println("Test set accuracy = " + accuracy)


  }
}
