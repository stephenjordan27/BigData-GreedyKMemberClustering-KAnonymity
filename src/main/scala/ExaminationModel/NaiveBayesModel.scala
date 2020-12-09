package ExaminationModel

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

class NaiveBayesModel {
  def create_encoded_features_dataframe(table: DataFrame,anonymizeTableWithoutLabel:DataFrame,label:String,isAnonym: Boolean):DataFrame = {
    // 1. Create label index and vector for each column
    val cols = anonymizeTableWithoutLabel.columns
    val encodedFeatures = cols.flatMap { columnName =>

      // Estimator: StringIndexer (create label index for each column from column value)
      val stringIndexer = new StringIndexer().setInputCol(columnName).setOutputCol(columnName + "_Index")

      // Transformer: OneHotEncoderEstimator (create vector for each column from label index)
      val oneHotEncoder = new OneHotEncoderEstimator().setInputCols(Array(columnName + "_Index")).
                          setOutputCols(Array(columnName + "_vec")).
                          setDropLast(false)

      Array(stringIndexer.setHandleInvalid("keep"), oneHotEncoder)


    }


    // 2. Pipeline:  chains multiple Transformers and Estimators together to specify an ML workflow.
    val pipeline = new Pipeline().setStages(encodedFeatures)
    val indexer_model = pipeline.fit(anonymizeTableWithoutLabel)

    // 3. Create vector feature

    // Example: Sparse Vector
    val sparseVector = indexer_model.transform(anonymizeTableWithoutLabel)

    // Transformer: Vector Assembler (create feature vectors)
    val vecFeatures = sparseVector.columns.filter(_.contains("Index"))
    val vectorAssembler = new VectorAssembler().setInputCols(vecFeatures).setOutputCol("features")
    val pipelineVectorAssembler = new Pipeline().setStages(Array(vectorAssembler))
    var result_df = pipelineVectorAssembler.fit(sparseVector).transform(sparseVector) // hasil dataframe pipeline

    var name = label
    if(isAnonym){
      name = "Anonym_"+label
    }

    val x = table.select("id",name).withColumnRenamed("id","id_temp")
    val indexer = new StringIndexer().setInputCol(name).setOutputCol(name + "_Index").fit(x)
    val y = indexer.transform(x)

    result_df = result_df.join(y,result_df("id")===x("id_temp"))

    return result_df

  }

  def model_training(table: DataFrame, json: DataFrame):DataFrame={
    val label = json.select("naive_bayes.label").first().getString(0)
    val trainingSet = json.select("naive_bayes.training_set").first().getDouble(0)
    val testSet = json.select("naive_bayes.test_set").first().getDouble(0)
    val pathModel:String = json.select("naive_bayes.path_model_normal").first().getString(0)

    val Array(training, test) = table.randomSplit(Array(trainingSet, testSet))
    val model = new NaiveBayes().setModelType("multinomial").setLabelCol(label+"_Index").fit(training)
    model.write.overwrite.save(pathModel)

    val predictions = model.transform(test)
    return predictions
  }


  def model_training_anonym(table: DataFrame, json: DataFrame):DataFrame={
    val label = json.select("naive_bayes.label").first().getString(0)
    val trainingSet = json.select("naive_bayes.training_set").first().getDouble(0)
    val testSet = json.select("naive_bayes.test_set").first().getDouble(0)
    val pathModel:String = json.select("naive_bayes.path_model_anonym").first().getString(0)

    val Array(training, test) = table.randomSplit(Array(trainingSet, testSet))
    val model = new NaiveBayes().setModelType("multinomial").setLabelCol("Anonym_"+label+"_Index").fit(training)
    model.write.overwrite.save(pathModel)

    val predictions = model.transform(test)
    return predictions
  }

  def model_evaluation(predictions:DataFrame, json: DataFrame):Double={
    val attrName = json.select("naive_bayes.label").first().getString(0)
    val evaluator = new MulticlassClassificationEvaluator()
                        .setLabelCol(attrName+"_Index")
                        .setPredictionCol("prediction")
                        .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    return accuracy
  }

  def model_evaluation_anonym(predictions:DataFrame, json: DataFrame):Double={
    val attrName = json.select("naive_bayes.label").first().getString(0)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("Anonym_"+attrName+"_Index")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    return accuracy
  }

  def comparing_model_evaluation_csv(spark:SparkSession,accuracyNormalTable:Double,accuracyAnonymizeTable:Double):DataFrame={
    import spark.implicits._
    val output = Seq(
      ("Normal table",accuracyNormalTable),
      ("Anonymize table",accuracyAnonymizeTable),
      ("How much different is accuracy?",Math.abs(accuracyNormalTable-accuracyAnonymizeTable))
    ).toDF("Info","Accuracy")

    return output
  }

}
