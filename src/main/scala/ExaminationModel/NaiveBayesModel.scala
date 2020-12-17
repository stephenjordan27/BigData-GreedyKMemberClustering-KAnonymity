package ExaminationModel

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

class NaiveBayesModel {
  def create_encoded_features_dataframe(anonymizeTable: DataFrame,anonymizeTableWithoutLabel:DataFrame,label:String,isAnonym: Boolean):DataFrame = {
    // 1. Create label index and vector for each column
    val cols = anonymizeTableWithoutLabel.drop("id").columns
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

    val x = anonymizeTable.select("id",name).withColumnRenamed("id","id_temp")
    val indexer = new StringIndexer().setInputCol(name).setOutputCol(name + "_Index").fit(x)
    val y = indexer.transform(x)

    result_df = result_df.join(y,result_df("id")===x("id_temp"))
    result_df = result_df.drop("id_temp")

    return result_df

  }

  def create_label_column(spark:SparkSession,table: DataFrame, json: DataFrame, name: String):DataFrame={
    table.createOrReplaceTempView("tCredit")
    val label = json.select("naive_bayes.label").first().getString(0)
    val median = spark.sql("SELECT CAST(percentile_approx("+label+", 0.5) AS DOUBLE) as Q2 FROM tCredit").first().getDouble(0).toInt
    val result = table.withColumn(name+label+"_label",getLabelFromNumeric(median)(table(label)))
    return result
  }

  def getLabelFromNumeric(median:Int) = udf ( (num: Int) => {
    if(num <= median){
      "<="+median/1000+"k"
    }
    else{
      ">"+median/1000+"k"
    }
  })

  def model_training(table: DataFrame, json: DataFrame, label:String):DataFrame={
    val trainingSet = json.select("naive_bayes.training_set").first().getDouble(0)
    val testSet = json.select("naive_bayes.test_set").first().getDouble(0)
    val pathModel:String = json.select("naive_bayes.model_path_normal").first().getString(0)

    val Array(training, test) = table.randomSplit(Array(trainingSet, testSet))
    val model = new NaiveBayes().setModelType("multinomial").setLabelCol(label+"_Index").fit(training)
    model.write.overwrite.save(pathModel) // tulis model ke file

    val predictions = model.transform(test)
    return predictions
  }


  def model_training_anonym(table: DataFrame, json: DataFrame,label:String):DataFrame={
    val trainingSet = json.select("naive_bayes.training_set").first().getDouble(0)
    val testSet = json.select("naive_bayes.test_set").first().getDouble(0)
    val pathModel:String = json.select("naive_bayes.model_path_anonym").first().getString(0)

    val Array(training, test) = table.randomSplit(Array(trainingSet, testSet))
    val model = new NaiveBayes().setModelType("multinomial").setLabelCol("Anonym_"+label+"_Index").fit(training)
    model.write.overwrite.save(pathModel)

    val predictions = model.transform(test)
    return predictions
  }

  def model_evaluation(predictions:DataFrame, json: DataFrame,label:String):Double={
    val evaluator = new MulticlassClassificationEvaluator()
                        .setLabelCol(label+"_Index")
                        .setPredictionCol("prediction")
                        .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    return accuracy
  }

  def model_evaluation_anonym(predictions:DataFrame, json: DataFrame,label:String):Double={
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("Anonym_"+label+"_Index")
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
