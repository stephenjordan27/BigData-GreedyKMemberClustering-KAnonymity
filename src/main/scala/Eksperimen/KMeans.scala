package Eksperimen

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object KMeans {

  def main(args:Array[String]): Unit = {

    // source: https://medium.com/knoldus/a-sample-ml-pipeline-for-clustering-in-spark-98e3ba74fdf9
    // The sequence of task involves in k-means modelling :
    // 1. Converting categorical attribute labels into label indexes
    // 2. Converting categorical label indexes into numerical vectors
    // 3. Combining all numerical features into a single feature vector
    // 4. Fitting a K-Means model on the extracted features
    // 5. Predicting using K-Means model to get clusters for each data row

    val spark = SparkSession
      .builder.master("local[*]")
      .appName("KMeans")
      .getOrCreate()

    // DATA EXPLORATION

    // 1. Looking at schema
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

    // 2. Read the data
    val adult100k_df = spark.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(schema)
      .load("input/adult100k.csv")

    // DATA PREPARATION
    // Estimator: trains on a DataFrame and produces a model.
    // Transformer: transforms a DataFrame with features into a DataFrame with predictions.

    // 1. Create label index and vector for each column
    val cols = adult100k_df.columns

    val encodedFeatures = cols.flatMap { columnName =>

        // Estimator: StringIndexer (create label index for each column from column value)
        val stringIndexer = new StringIndexer()
          .setInputCol(columnName)
          .setOutputCol(columnName + "_Index")

        // Transformer: OneHotEncoderEstimator (create vector for each column from label index)
        val oneHotEncoder = new OneHotEncoderEstimator()
          .setInputCols(Array(columnName + "_Index"))
          .setOutputCols(Array(columnName + "_vec"))
          .setDropLast(false)

        Array(stringIndexer.setHandleInvalid("keep"),oneHotEncoder)
    }


    // 2. Pipeline:  chains multiple Transformers and Estimators together to specify an ML workflow.
    val pipeline = new Pipeline().setStages(encodedFeatures)
    val indexer_model = pipeline.fit(adult100k_df)

    // 3. Create vector feature

    // Example: Sparse Vector
    val sparseVector = indexer_model.transform(adult100k_df)

    // Example: Dense Vector
    val sparseToDense = udf((v: Vector) => v.toDense)
    val denseVector = sparseVector.withColumn("dense_workclass_vec",sparseToDense(sparseVector("workclass_vec")))

    // Transformer: Vector Assembler (create feature vectors)
    val vecFeatures = sparseVector.columns.filter(_.contains("Index"))
    val vectorAssembler = new VectorAssembler().setInputCols(vecFeatures).setOutputCol("features")
    val pipelineVectorAssembler = new Pipeline().setStages(Array(vectorAssembler))
    val result_df = pipelineVectorAssembler.fit(sparseVector).transform(sparseVector) // hasil dataframe pipeline

    // MODEL TRAINING

    // 1. Trains a k-means model.
    val kmeans = new KMeans().setK(8).setFeaturesCol("features").setPredictionCol("prediction")
    val model = kmeans.fit(result_df)

    // 2. Make predictions
    val predictions  = model.transform(result_df)
    predictions.select("age","workclass","education","occupation","sex","salary","prediction").show(10)

    // MODEL EVALUATION

    // 1. Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // OPTIONAL

    // Save model
    model.write.overwrite().save("/input/KMeansModel")

    // Load model
    val model_load = KMeansModel.load("input/KMeansModel")

  }

}
