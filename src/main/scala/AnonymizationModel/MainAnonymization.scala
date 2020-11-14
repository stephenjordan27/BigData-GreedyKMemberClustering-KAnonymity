package AnonymizationModel

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object MainAnonymization  {


  def main(args:Array[String]): Unit = {


    val spark = SparkSession.builder
      .master("local[2]")
//      .config("spark.shuffle.memoryFraction","0.1")
//      .config("spark.storage.memoryFraction","0.1")
//      .config("spark.driver.memoryOverhead","2g")
//      .config("spark.driver.memory", "4g")
//      .config("spark.memory.offHeap.enabled",true)
//      .config("spark.memory.offHeap.size","3g")
//      .config("spark.maxRemoteBlockSizeFetchToMem","1g")
//      .config("spark.locality.wait","60000")
      .config("spark.driver.memory","4g")
      .appName("Anonymization with Big Data")
      .getOrCreate()

    val sc = spark.sparkContext



//    spark.conf.set("spark.executor.instances", "4")
//    spark.conf.set("spark.executor.cores", "5");
//
//    implicit val NO_OF_EXECUTOR_INSTANCES = spark.conf.get("spark.executor.instances").toInt
//    implicit val NO_OF_EXECUTOR_CORES = spark.conf.get("spark.executor.cores").toInt
//
//    val idealPartionionNumber = NO_OF_EXECUTOR_INSTANCES * NO_OF_EXECUTOR_CORES

    // Parameter Greedy K-Member Clustering
    val path_JSON = args(0)
    val json:DataFrame = spark.read.option("multiline", "true").json(path_JSON).cache() // ganti di sini
    val path_data_input = json.select("input_path").first().getString(0)
    val path_data_output = json.select("output_path").first().getString(0)
    val datasetInput = spark.read.format("csv").option("header", "true").load(path_data_input)
    val columnSelectionWithID = generate_dataframe_from_csv(spark,json,datasetInput)
    val k:Int = json.select("k").first().getLong(0).toInt
    val listDataType:Array[DataType] = columnSelectionWithID.schema.fields.map(f => f.dataType)


    // Membatasi record yang dipakai untuk eksperimen
    val numSampleDatas = json.select("num_sample_datas").first().getLong(0).toInt
    val S = columnSelectionWithID.where("id <= "+numSampleDatas).cache()
    S.coalesce(1)
      .write
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .csv(path_data_output+"normal-table")


    // 1. Melakukan pengelompokan data dengan algoritma Greedy k-member clustering
    val GKMC = new GreedyKMemberClustering() // 4 menit
    val gkmcDF = GKMC.greedy_k_member_clustering(spark,sc,json,S,k,listDataType)

    // 2. Menyimpan hasil pengelompokan data ke dalam CSV

    gkmcDF.coalesce(1)
          .write
          .option("header","true")
          .option("sep",",")
          .mode("overwrite")
          .csv(path_data_output+"greedy-k-member-clustering")

    S.unpersist()
    json.unpersist()

    // 3. Melakukan anonimisasi pada data yang telah dikelompokan menggunakan k-anonymity
    val KAnonymity = new KAnonymity()
    val kanonymityDF = KAnonymity.k_anonymity(spark,json,gkmcDF,listDataType)

    // 4. Menyimpan hasil pengelompokan data ke dalam CSV
    kanonymityDF.coalesce(1)
                .write
                .option("header","true")
                .option("sep",",")
                .mode("overwrite")
                .csv(path_data_output+"k-anonymity")


  }

  def generate_dataframe_from_csv(spark:SparkSession, json:DataFrame, dataInput: DataFrame):DataFrame={
    val quasiIdentifier:ListBuffer[Seq[String]] = read_element_from_json(json,"quasi_identifier")
    val sensitiveIdentifier:ListBuffer[Seq[String]] = read_element_from_json(json,"sensitive_identifier")

    val quasiIdentifierAttribute:List[String] = get_attribute_name_json(quasiIdentifier).toList
    val quasiIdentifierDatatype:List[String] = get_attribute_datatype_json(quasiIdentifier).toList

    val sensitiveIdentifierAttribute = get_attribute_name_json(sensitiveIdentifier)
    val sensitiveIdentifierDatatype = get_attribute_datatype_json(sensitiveIdentifier)

    val selectedColumnAttribute:List[String] = quasiIdentifierAttribute ++ sensitiveIdentifierAttribute distinct
    val selectedColumnDatatype = quasiIdentifierDatatype ++ sensitiveIdentifierDatatype

    var columnSelectionDF = dataInput.select(selectedColumnAttribute.head,selectedColumnAttribute.tail: _*).toDF() // ganti di sini

    // Buat bikin schema DataFrame
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

    return result
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
      val quasiIdentifier = json.select(element).collect()
      val quasiIdentifierArr = quasiIdentifier.map(row => row.getSeq[Row](0))
      quasiIdentifierArr.foreach(quasiIdentifierVariables => {
        quasiIdentifierVariables.map(row => {
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
