package AnonymizationModel

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object MainAnonymization  {


  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder
                .master("local[2]")
                .appName("Anonymization with Big Data")
                .getOrCreate()

    val path_HDFS_CLUSTER= "hdfs://master:9070/skripsi-jordan/temp"
    val path_HDFS_LOCAL= "hdfs://localhost:50071/skripsi"
    val path_HDFS = path_HDFS_LOCAL

    val path_delete_HDFS_CLUSTER= "/skripsi-jordan/temp"
    val path_delete_HDFS_LOCAL= "/skripsi"
    val path_delete_HDFS = path_delete_HDFS_LOCAL

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

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:50071/"), hadoopConf)

    // 1. Melakukan pengelompokan data dengan algoritma Greedy k-member clustering
    val GKMC = new GreedyKMemberClustering() // 4 menit
    val listBinaryTree = create_list_binary_tree_attribute(S,json)
    val gkmcDF = GKMC.greedy_k_member_clustering(spark,json,S,k,numSampleDatas,listBinaryTree,hdfs,path_HDFS,path_delete_HDFS).cache()

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
    val kanonymityDF = KAnonymity.k_anonymity(spark,json,gkmcDF,listDataType,listBinaryTree,hdfs)

    // 4. Menyimpan hasil pengelompokan data ke dalam CSV
    kanonymityDF.coalesce(1)
                .write
                .option("header","true")
                .option("sep",",")
                .mode("overwrite")
                .csv(path_data_output+"k-anonymity")


  }

  def read_dgh_from_json(json: DataFrame,category: String): ListBuffer[Seq[String]] = {
    var result = ListBuffer[Seq[String]]()

    try{
      val treeName = json.select("domain_generalization_hierarchy."+ category+".tree").collect()(0).getString(0)
      val dgh_json = json.select("domain_generalization_hierarchy." + category+".generalization")
      val dgh = dgh_json.collect()
      val dghArr = dgh.map(row => row.getSeq[Row](0))
      result += Seq[String](treeName)
      dghArr.foreach(dgh_variables => {
        dgh_variables.map(row => {
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

  def create_list_binary_tree_attribute(S: DataFrame, json: DataFrame): ListBuffer[BinaryTree] = {
    val listBinaryTree = new ListBuffer[BinaryTree]
    S.columns.foreach{colName =>
      val dgh = read_dgh_from_json(json,colName)
      if(dgh!=null){
        val treeName = dgh.remove(0)(0)
        listBinaryTree += create_binary_tree_from_dgh_attribute(dgh)
      }
      else{
        listBinaryTree += null
      }
    }
    return listBinaryTree
  }

  def create_binary_tree_from_dgh_attribute(dgh: ListBuffer[Seq[String]]):BinaryTree = {
    val tree = new BinaryTree
    val queue = new mutable.Queue[Node]()
    var currentNode = new Node("",0)
    var initialize = true

    //////////////////////////////////////////////baris ini diganti//////////////////////////////////////////////////
    dgh.foreach { attribute =>  // looping
      val level = attribute(0).toInt
      val parent = attribute(1)
      val position = attribute(2)
      val value = attribute(3).toString

      if(queue.size == 0){
        var newNode = new Node(value,level) // ngasih nilai
        tree.root = newNode
        queue.enqueue(newNode)
      }
      else {
        if(parent != currentNode.name || initialize){
          currentNode = queue.dequeue()
        }
        if(position == "left"){
          currentNode.left = new Node(value,level)
          queue.enqueue(currentNode.left)
        }
        else{
          currentNode.right = new Node(value,level)
          queue.enqueue(currentNode.right)
        }
        initialize = false
      }
    }
    //////////////////////////////////////////////baris ini diganti//////////////////////////////////////////////////
    return tree
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
