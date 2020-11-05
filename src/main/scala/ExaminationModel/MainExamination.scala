package ExaminationModel

import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object MainExamination {

  def main(args:Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local[*]")
      .appName("SparkSQL")
      .getOrCreate()


    // Parameter Pengujian
    val path_JSON = args(0)
    val json: DataFrame = spark.read.option("multiline", "true").json(path_JSON).cache() // ganti di sini

    val path_data_input_normal_table = json.select("input_path").first().getString(0)                           // ganti di sini
    val path_data_input_anonymize_table = json.select("input_path2").first().getString(0)

    val normalTable = spark.read.format("csv").option("header", "true").load(path_data_input_normal_table)
    val anonymizeTable = spark.read.format("csv").option("header", "true").load(path_data_input_anonymize_table)

    var normalTableWithID = generate_dataframe_from_csv(spark,json,normalTable)
    var anonymizeTableWithID = generate_dataframe_from_csv(spark,json,anonymizeTable)

    // Membatasi record yang dipakai untuk eksperimen
    val numSampleDatas = json.select("num_sample_datas").first().getLong(0).toInt
    normalTableWithID = normalTableWithID.where("id <= "+numSampleDatas)
    anonymizeTableWithID = anonymizeTableWithID.where("id <= "+numSampleDatas)

    // Implementasi Model
    val model_name = json.select("model_name").first().getString(0)

    // K-Means
    if(model_name.contains("k_means")){
      val k_means_model = new KMeansModel()

      // K-Means: Normal Table
      val encodedDFNormalTable = k_means_model.create_encoded_features_dataframe(normalTableWithID)
      val modelDFNormalTable = k_means_model.model_training(encodedDFNormalTable,json)
      val modelEvaluationNormalTable = k_means_model.model_evaluation(modelDFNormalTable)

      val listPredictorAttributeNormalTable = list_of_predictor_attribute(spark,modelDFNormalTable)
      val predictionDFNormalTable = modelDFNormalTable.select(listPredictorAttributeNormalTable.head, listPredictorAttributeNormalTable.tail: _*)

      // K-Means: Anonymize Table
      val encodedDFAnonymizeTable = k_means_model.create_encoded_features_dataframe(anonymizeTableWithID)
      val modelDFAnonymizeTable = k_means_model.model_training(encodedDFAnonymizeTable,json)
      val modelEvaluationAnonymizeTable = k_means_model.model_evaluation(modelDFAnonymizeTable)

      val listPredictorAttributeAnonymizeTable = list_of_predictor_attribute(spark,modelDFNormalTable)
      val predictionDFAnonymizeTable = modelDFNormalTable.select(listPredictorAttributeAnonymizeTable.head, listPredictorAttributeAnonymizeTable.tail: _*)

      // K-Means: Model Evaluation
      val model_evaluation = k_means_model.comparing_model_evaluation_csv(spark,modelEvaluationNormalTable,modelEvaluationAnonymizeTable)

      // K-Means: CSV
      val outputPath = json.select("output_path").first().getString(0)
      predictionDFNormalTable.coalesce(1) //Normal Table
        .write
        .option("header","true")
        .option("sep",",")
        .mode("overwrite")
        .csv(outputPath+"k-means/normal-table")
      predictionDFAnonymizeTable.coalesce(1) //Anonymize Table
        .write
        .option("header","true")
        .option("sep",",")
        .mode("overwrite")
        .csv(outputPath+"k-means/anonymize-table")
      model_evaluation.coalesce(1) //Silhoette Table
        .write
        .option("header","true")
        .option("sep",",")
        .mode("overwrite")
        .csv(outputPath+"k-means/silhouette-score")

    }
    // Naive Bayes
    else if(model_name.contains("naive_bayes")){

      val naive_bayes_model = new NaiveBayesModel()

      //Naive Bayes: Normal Table
      val encodedDFNormalTable = naive_bayes_model.create_encoded_features_dataframe(normalTableWithID)
      val modelDFNormalTable = naive_bayes_model.model_training(encodedDFNormalTable,json)
      val modelEvaluationNormalTable = naive_bayes_model.model_evaluation(modelDFNormalTable,json)

      val listPredictorAttributeNormalTable = list_of_predictor_attribute(spark,modelDFNormalTable)
      val predictionDFNormalTable = modelDFNormalTable.select(listPredictorAttributeNormalTable.head, listPredictorAttributeNormalTable.tail: _*)

      //Naive Bayes: Anonymize Table
      val encodedDFAnonymizeTable = naive_bayes_model.create_encoded_features_dataframe(anonymizeTableWithID)
      val modelDFAnonymizeTable = naive_bayes_model.model_training(encodedDFAnonymizeTable,json)
      val modelEvaluationAnonymizeTable = naive_bayes_model.model_evaluation(modelDFAnonymizeTable,json)

      val listPredictorAttributeAnonymizeTable = list_of_predictor_attribute(spark,modelDFAnonymizeTable)
      val predictionDFAnonymizeTable = modelDFAnonymizeTable.select(listPredictorAttributeAnonymizeTable.head, listPredictorAttributeAnonymizeTable.tail: _*)

      // K-Means: Model Evaluation
      val model_evaluation = naive_bayes_model.comparing_model_evaluation_csv(spark,modelEvaluationNormalTable,modelEvaluationAnonymizeTable)

      val outputPath = json.select("output_path").first().getString(0)
      predictionDFNormalTable.coalesce(1)
                  .write
                  .option("header","true")
                  .option("sep",",")
                  .mode("overwrite")
                  .csv(outputPath+"naive-bayes")
      predictionDFAnonymizeTable.coalesce(1) //Anonymize Table
        .write
        .option("header","true")
        .option("sep",",")
        .mode("overwrite")
        .csv(outputPath+"k-means/anonymize-table")
      model_evaluation.coalesce(1) //Silhoette Table
        .write
        .option("header","true")
        .option("sep",",")
        .mode("overwrite")
        .csv(outputPath+"k-means/silhouette-score")

    }

  }

  def list_of_predictor_attribute(spark: SparkSession, record: DataFrame): List[String] = {
    record.createOrReplaceTempView("tAdults")
    val test = spark.catalog.listColumns("tAdults").select("name","datatype")
    val df = test.select("name").
      where("datatype = 'string' " +
                          "or datatype = 'int' " +
                          "or datatype = 'double' " +
                          "and name not like '%Index%'")
    val columnName = df.collect().map(_(0)).toList.asInstanceOf[List[String]]

    return columnName
  }

  def generate_dataframe_from_csv(spark:SparkSession, json:DataFrame, dataInput: DataFrame):DataFrame={
    val selectedColumn:ListBuffer[Seq[String]] = read_element_from_json(json,"selected_column")

    val selectedColumnAttribute:List[String] = get_attribute_name_json(selectedColumn).toList
    val selectedColumnDatatype:List[String] = get_attribute_datatype_json(selectedColumn).toList

    var columnSelectionDF = dataInput.select(selectedColumnAttribute.head,selectedColumnAttribute.tail: _*).toDF() // ganti di sini

    for(i <- 0 until selectedColumnAttribute.length){

      val attrName = selectedColumnAttribute(i)
      val datatype = selectedColumnDatatype(i)


      if(datatype == "category"){
        columnSelectionDF = columnSelectionDF.withColumn(attrName+"_new",columnSelectionDF(attrName).cast(StringType))
        columnSelectionDF = columnSelectionDF.drop(attrName)
      }
      else{
        columnSelectionDF = columnSelectionDF.withColumn(attrName+"_new",columnSelectionDF(attrName).cast(IntegerType))
        columnSelectionDF = columnSelectionDF.drop(attrName)
      }

    }

    val columnSelectionRenamedDF = columnSelectionDF.toDF(selectedColumnAttribute: _*)

    return columnSelectionRenamedDF
  }

  def get_attribute_name_json(values: ListBuffer[Seq[String]]): ListBuffer[String] ={
    var result = ListBuffer[String]()
    for(value <- values) {
      result += value(0)
    }
    return result
  }

  def get_attribute_datatype_json(values: ListBuffer[Seq[String]]): ListBuffer[String] ={
    var result = ListBuffer[String]()
    for(value <- values) {
      result += value(1)
    }
    return result
  }

  def read_element_from_json(json: DataFrame,element: String): ListBuffer[Seq[String]] = {
    var result = ListBuffer[Seq[String]]()

    try{
      val selectedColumn = json.select(element).collect()
      val selectedColumnrArr = selectedColumn.map(row => row.getSeq[Row](0))
      selectedColumnrArr.foreach(selectedColumnVariables => {
        selectedColumnVariables.map(row => {
          result += row.toSeq.asInstanceOf[Seq[String]]
        })
      })
      return result
    }
    catch {
      case x: Exception => {
        result = null
        return result
      }
    }

  }

}
