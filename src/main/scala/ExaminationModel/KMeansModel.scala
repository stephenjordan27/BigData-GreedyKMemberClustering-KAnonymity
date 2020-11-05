package ExaminationModel

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

class KMeansModel {

  def create_encoded_features_dataframe(table: DataFrame):DataFrame = {
    // 1. Create label index and vector for each column
    val cols = table.columns
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
    val indexer_model = pipeline.fit(table)

    // 3. Create vector feature

    // Example: Sparse Vector
    val sparseVector = indexer_model.transform(table)

    // Transformer: Vector Assembler (create feature vectors)
    val vecFeatures = sparseVector.columns.filter(_.contains("Index"))
    val vectorAssembler = new VectorAssembler().setInputCols(vecFeatures).setOutputCol("features")
    val pipelineVectorAssembler = new Pipeline().setStages(Array(vectorAssembler))
    val result_df = pipelineVectorAssembler.fit(sparseVector).transform(sparseVector) // hasil dataframe pipeline

    return result_df
  }


  def model_training(table: DataFrame, json: DataFrame):DataFrame={
    val k = json.select("k_means.k").first().getLong(0).toInt
    val kmeans = new KMeans().setK(k).setFeaturesCol("features").setPredictionCol("prediction")
    val model = kmeans.fit(table)
    val predictions  = model.transform(table)
    return predictions
  }

  def model_evaluation(predictions:DataFrame):Double={
    val evaluator = new ClusteringEvaluator()
    val silhouette_score = evaluator.evaluate(predictions)
    return silhouette_score
  }

  def comparing_model_evaluation_csv(spark:SparkSession,silhouetteScoreNormalTable:Double,silhouetteScoreAnonymizeTable:Double):DataFrame={
    import spark.implicits._
    val output = Seq(
      ("Normal table",silhouetteScoreNormalTable),
      ("Anonymize table",silhouetteScoreAnonymizeTable),
      ("How much different is Silhouette score?",Math.abs(silhouetteScoreNormalTable-silhouetteScoreAnonymizeTable))
    ).toDF("Info","Silhouette score")

    return output
  }

}
