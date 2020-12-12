package ExaminationModel


import AnonymizationModel.{BinaryTree, LowestCommonAncestor}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object MainExamination {

  def main(args:Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local[*]")
      .appName("SparkSQL")
      .getOrCreate()

    val sc = spark.sparkContext


    // Parameter Pengujian
    val path_JSON = args(0)
    val json: DataFrame = spark.read.option("multiline", "true").json(path_JSON).cache() // ganti di sini
    val num_sample_datas = json.select("num_sample_data").first().getLong(0).toInt
    val path_data_input_normal_table = json.select("input_path_normal").first().getString(0)                           // ganti di sini
    val path_data_input_anonymize_table = json.select("input_path_anonymize").first().getString(0)
    val path_data_input_cluster_table = json.select("input_path_cluster").first().getString(0)
    val outputPath = json.select("output_path").first().getString(0)
    val model_name = json.select("model_name").first().getString(0)

    val normalTable = spark.read.format("csv").option("header", "true").load(path_data_input_normal_table)
    val anonymizeTable = spark.read.format("csv").option("header", "true").load(path_data_input_anonymize_table)
    val clusterTable = spark.read.format("csv").option("header", "true").load(path_data_input_cluster_table)


    val normalTableWithID = generate_dataframe_from_csv(spark,json,normalTable, false).
                            where("id <= "+num_sample_datas).cache()
    val anonymizeTableWithID =  anonymizeTable.
                                where("id <= "+num_sample_datas).cache()
    val clusterTableWithID =  generate_dataframe_from_csv(spark,json,clusterTable, true).
                              where("id <= "+num_sample_datas).cache()


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

      val listPredictorAttributeAnonymizeTable = list_of_predictor_attribute(spark,modelDFAnonymizeTable)
      val predictionDFAnonymizeTable = modelDFAnonymizeTable.select(listPredictorAttributeAnonymizeTable.head, listPredictorAttributeAnonymizeTable.tail: _*)

      // K-Means: Model Evaluation
      val model_evaluation = k_means_model.comparing_model_evaluation_csv(spark,modelEvaluationNormalTable,modelEvaluationAnonymizeTable)

      // CSV
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

      val label = json.select("naive_bayes.label").first().getString(0)
      val naive_bayes_model = new NaiveBayesModel()

      //Naive Bayes: Normal Table
      val normalTableWithoutLabel = normalTableWithID.drop(label)
      val encodedDFNormalTable = naive_bayes_model.create_encoded_features_dataframe(normalTableWithID,normalTableWithoutLabel,label,false)
      val modelDFNormalTable = naive_bayes_model.model_training(encodedDFNormalTable,json)
      val modelEvaluationNormalTable = naive_bayes_model.model_evaluation(modelDFNormalTable,json)


      //Naive Bayes: Anonymize Table
      val anonymizeTableWithoutLabel = anonymizeTableWithID.drop("Anonym_"+label)
      val encodedDFAnonymizeTable = naive_bayes_model.create_encoded_features_dataframe(anonymizeTableWithID,anonymizeTableWithoutLabel,label,true)
      val modelDFAnonymizeTable = naive_bayes_model.model_training_anonym(encodedDFAnonymizeTable,json)
      val modelEvaluationAnonymizeTable = naive_bayes_model.model_evaluation_anonym(modelDFAnonymizeTable,json)


      // Naive Bayes: Model Evaluation
      val model_evaluation = naive_bayes_model.comparing_model_evaluation_csv(spark,modelEvaluationNormalTable,modelEvaluationAnonymizeTable)

      // CSV
      val outputPath = json.select("output_path").first().getString(0)
      val pathModelNormal:String = json.select("naive_bayes.path_model_normal").first().getString(0)
      val pathModelAnonym:String = json.select("naive_bayes.path_model_anonym").first().getString(0)
      val loadModelNormalTable = NaiveBayesModel.load(pathModelNormal)
      val loadModelAnonymTable = NaiveBayesModel.load(pathModelAnonym)

      val normalPredictionTable = normalTableWithID.drop(label)
      val anonymPredictionTable = anonymizeTableWithID.drop("Anonym_"+label)

      val encodedNormalPredictionTable = naive_bayes_model.create_encoded_features_dataframe(normalTableWithID,normalPredictionTable,label,false)
      val encodedAnonymPredictionTable = naive_bayes_model.create_encoded_features_dataframe(anonymizeTableWithID,anonymPredictionTable,label,true)

      val predictionDFNormalTable = loadModelNormalTable.transform(encodedNormalPredictionTable)
      val predictionDFAnonymizeTable = loadModelAnonymTable.transform(encodedAnonymPredictionTable)

      val predictionNormalCSV =   predictionDFNormalTable.select(predictionDFNormalTable.columns.filter(colName =>
                                  !colName.contains("_Index") && !colName.contains("_vec") &&
                                  !colName.contains("rawPrediction") && !colName.contains("probability") &&
                                  !colName.contains("id_temp") && !colName.contains("features")).
                                  map(predictionDFNormalTable(_)) : _*)

      val predictionAnonymizeCSV =  predictionDFAnonymizeTable.select(predictionDFAnonymizeTable.columns.filter(colName =>
                                    !colName.contains("_Index") && !colName.contains("_vec") &&
                                    !colName.contains("rawPrediction") && !colName.contains("probability") &&
                                    !colName.contains("id_temp") && !colName.contains("features")).
                                    map(predictionDFAnonymizeTable(_)) : _*)


      predictionNormalCSV.sort("id").coalesce(1) //Normal Table
                  .write
                  .option("header","true")
                  .option("sep",",")
                  .mode("overwrite")
                  .csv(outputPath+"naive-bayes/normal-table")
      predictionAnonymizeCSV.sort("id").coalesce(1) //Anonymize Table
                  .write
                  .option("header","true")
                  .option("sep",",")
                  .mode("overwrite")
                  .csv(outputPath+"naive-bayes/anonymize-table")
      model_evaluation.coalesce(1) //Accuracy
                  .write
                  .option("header","true")
                  .option("sep",",")
                  .mode("overwrite")
                  .csv(outputPath+"naive-bayes/accuracy")

    }
    else if(model_name.contains("total_information_loss")){
      // Parameter Total Information Loss
      var total_information_loss:Double = 0.0

      // Parameter JSON
      val path_json = json.select("total_information_loss.path_json").first().getString(0)
      val json2 = spark.read.option("multiline", "true").json(path_json).cache()
      val hdfs_name = json2.select("hdfs").first().getString(0)
      val temp_files = json2.select("temp_files").first().getString(0)

      // Inisialisasi path HDFS untuk membaca csv dan delete folder
      val path_HDFS = hdfs_name + temp_files
      val path_delete_function_HDFS = temp_files

      // Membaca input k-anonymity dari HDFS
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfs_name), hadoopConf)

      // Mengambil file GKMC
      val clusters_schema = clusterTableWithID.schema

      // Tulis clusters ke file HDFS
      var numClusters = clusterTableWithID.select("Cluster").distinct().count().toInt
      this.delete_folder_hdfs(path_delete_function_HDFS+"/totalIL1_tmp/",hdfs)
      clusterTableWithID.coalesce(1).write.option("header", "true").
                          csv(path_HDFS+"/totalIL1_tmp/")

      // Loop
      while(numClusters > 0){

        // Baca clusters dari file HDFS
        this.delete_folder_hdfs(path_delete_function_HDFS+"/totalIL1/",hdfs)
        hdfs.rename(new Path(path_HDFS+"/totalIL1_tmp"),new Path(path_HDFS+"/totalIL1"))
        var clusters_temp = spark.read.format("csv").option("header", "true").
                            schema(clusters_schema).load(path_HDFS+"/totalIL1/")

        // Mengambil sebuah cluster
        val clusterName = "Cluster "+numClusters
        val clusterDF = clusters_temp.where(clusters_temp("Cluster").contains(clusterName))
        val cluster_size = clusterDF.count().toInt
        val clusterDF_temp = clusterDF

        // Menulis cluster pada file HDFS
        this.delete_folder_hdfs(path_delete_function_HDFS+"/totalIL2_tmp/",hdfs)
        clusterDF.coalesce(1).write.option("header", "true").csv(path_HDFS+"/totalIL2_tmp/")

        // Melakukan perhitungan Total Information Loss
        total_information_loss += calculate_total_information_loss_optimize(json,clusterDF,cluster_size)

        // Membuang cluster yang sudah pernah dianonimisasi
        clusters_temp = clusters_temp.except(clusterDF_temp).cache()
        clusters_temp.coalesce(1).write.option("header", "true").
                      csv(path_HDFS+"/totalIL1_tmp/")
        if(numClusters == 0){
          this.delete_folder_hdfs(path_delete_function_HDFS+"/totalIL1/",hdfs)
          hdfs.rename(new Path(path_HDFS+"/totalIL1_tmp"), new Path(path_HDFS+"/totalIL1"))
        }

        numClusters -= 1


      }

      import spark.implicits._

      val result = Seq((total_information_loss)).toDF("total_information_loss")

      result.coalesce(1) //Accuracy
            .write
            .option("header","true")
            .option("sep",",")
            .mode("overwrite")
            .csv(outputPath+"total-infoloss")

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

  def generate_dataframe_from_csv(spark:SparkSession, json:DataFrame, dataInput: DataFrame, isCluster: Boolean):DataFrame={
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

    import org.apache.spark.sql.functions._

    val columnSelectionWithID = columnSelectionRenamedDF.withColumn("id_temp", row_number().over(Window.orderBy(monotonically_increasing_id())) )
    var columnID = columnSelectionWithID.select("id_temp")
    columnID = columnID.withColumnRenamed("id_temp","id")
    var result = columnID.join(columnSelectionWithID, columnID("id") === columnSelectionWithID("id_temp"),"inner")
    result = result.drop("id_temp")

    if(isCluster){
      val columnCluster = dataInput.select("id","Cluster").withColumnRenamed("id","id_temp")
      result = result.join(columnCluster,result("id")===columnCluster("id_temp"),"inner")
      result = result.drop("id_temp")
    }

    return result
  }



  def get_attribute_name_json(values: ListBuffer[Seq[String]]): ListBuffer[String] ={
    var result = ListBuffer[String]()
    for(value <- values) {
      result += value(0)
    }
    return result
  }

  def get_anonymize_attribute_name_json(values: ListBuffer[Seq[String]]): ListBuffer[String] ={
    var result = ListBuffer[String]()
    for(value <- values) {
      result += "Anonym_"+value(0)
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

  def get_anonymize_attribute_datatype_json(values: ListBuffer[Seq[String]]): ListBuffer[String] ={
    var result = ListBuffer[String]()
    for(value <- values) {
      result += "Anonym_"+value(1)
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

  def calculate_total_information_loss_optimize(json: DataFrame, cluster: DataFrame, cluster_size:Int): Double = {
    var cluster_temp = min_max_cluster(cluster)

    cluster.dtypes.filter(!_._1.contains("_")).foreach(element =>
      if(!element._1.contains("id") && !element._1.contains("Cluster") ){
        if(element._2.contains("Integer")){
          cluster_temp = cluster_temp.withColumn("IL_" + element._1, calculateNumericalInformationLossUnion(cluster_size)(col("max_"+element._1),col("min_"+element._1)))
        }
        else{
          cluster_temp = cluster_temp.withColumn("IL_" + element._1, lit(1))
        }
      }
    )

    val infoloss_union = cluster_temp.select(cluster_temp.columns.filter(_.contains("IL_")).map(cluster_temp(_)): _*)
    val sum_infoloss_union = infoloss_union.columns.toSeq.map(col _)

    cluster_temp = cluster_temp.withColumn("TotalIL",sum_(sum_infoloss_union: _*)*cluster_size)

    cluster_temp = cluster_temp.drop(cluster_temp.columns.filter(_.startsWith("IL")): _*)

    val result = cluster_temp.select("TotalIL").first().getDouble(0)

    return result

  }

  def calculateNumericalInformationLossUnion(cluster_size:Int) = udf ( (max_c:Int, min_c:Int) => {
    Math.abs(max_c-min_c)*1.0/cluster_size
  })

  def calculateCategoricalDistance(binaryTree: BinaryTree):UserDefinedFunction = udf( (category1: String, category2: String) => {
    if(binaryTree!=null){
      val node1 = binaryTree.search(category1)
      val node2 = binaryTree.search(category2)
      if(node1 != null && node2 != null && node1.name != node2.name){
        val LCA = new LowestCommonAncestor()
        val LCA_root_name = LCA.findLCA(binaryTree.root, node1.name, node2.name)
        val H_subtree = binaryTree.search(LCA_root_name).level
        val H_TD = binaryTree.getHeight(binaryTree.root).toDouble
        BigDecimal(H_subtree*1.0 / H_TD).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      }
      else{
        1.0
      }
    }
    else{
      1.0
    }

  })

  def sum_(cols: Column*) = cols.foldLeft(lit(0))(_ + _)

  def min_max_cluster(c: DataFrame):DataFrame = {
    var result:DataFrame = null
    c.dtypes.filter(element => element._2 == "IntegerType").foreach{element =>
      if(element._1 != "id" && element._2.contains("Integer")){

        if(result == null){
          result = c.select(max(element._1).as("max_"+element._1),min(element._1).as("min_"+element._1))
        }
        else{
          val c_temp = c.select(max(element._1).as("max_"+element._1),min(element._1).as("min_"+element._1))
          result = result.crossJoin(c_temp)
        }

      }

    }
    return result
  }

  def delete_folder_hdfs(pathName: String,hdfs:FileSystem) {
    val path = new Path(pathName)
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)
    }
  }



}
