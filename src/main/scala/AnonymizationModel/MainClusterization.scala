package AnonymizationModel

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object MainClusterization  {

  //    val path_HDFS_CLUSTER= "hdfs://master:9070/skripsi-jordan/temp"
  //    val path_HDFS_LOCAL= "hdfs://localhost:50071/skripsi"
  //    val path_delete_HDFS_CLUSTER= "/skripsi-jordan/temp"
  //    val path_delete_HDFS_LOCAL= "/skripsi"

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder
                .master("local[2]")
                .appName("GKMC")
                .getOrCreate()

    // Parameter JSON
    val path_JSON = args(0)
    val json:DataFrame = spark.read.option("multiline", "true").json(path_JSON).cache() // ganti di sini

    // Parameter Greedy K-Member Clustering
    val k = json.select("k").first().getLong(0).toInt
    val num_sample_datas = json.select("num_sample_datas").first().getLong(0).toInt
    val hdfs_name = json.select("hdfs").first().getString(0)
    val temp_files = json.select("temp_files").first().getString(0)
    val input_path = json.select("input_path").first().getString(0)
    val output_path = json.select("output_path").first().getString(0)

    // Konfigurasi HDFS
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfs_name), hadoopConf)

    // Dataset dengan ID
    val datasetInput = spark.read.format("csv").option("header", "true").load(input_path)
    val columnSelectionWithID = generate_dataframe_from_csv(spark,json,datasetInput)

    // Membatasi record yang dipakai untuk eksperimen
    val S = columnSelectionWithID.where("id <= "+num_sample_datas).repartition(45).cache()
    S.coalesce(1).write.option("header","true").option("sep",",").
                                mode("overwrite").csv(output_path+"normal-table")


    S.rdd.mapPartitionsWithIndex((x,y) => {println(s"partitions $x has ${y.length} records");y.map(a => a+"")}).collect.foreach(println)
    // Inisialisasi path HDFS untuk membaca csv dan delete folder
    val path_HDFS = hdfs_name + temp_files
    val path_delete_function_HDFS = temp_files

    // Melakukan pengelompokan data dengan algoritma Greedy k-member clustering
    val GKMC = new GreedyKMemberClustering() // 4 menit
    val listBinaryTree = create_list_binary_tree_attribute(S,json)
    val gkmcDF = GKMC.greedy_k_member_clustering(spark,json,S,k,num_sample_datas,listBinaryTree,
                 hdfs,path_HDFS,path_delete_function_HDFS).cache()

    // Menyimpan hasil pengelompokan data ke dalam CSV
    val kanonymity_input =  spark.read.format("csv").option("header", "true").
                            schema(gkmcDF.schema).load(path_HDFS+"/gkmc2/")
    val gkmc_output = kanonymity_input.select(kanonymity_input.columns.
                      filter(colName => !colName.startsWith("max_") &&
                      !colName.contains("min_")).map(kanonymity_input(_)) : _*)

    // Menyimpan input k-anonymity di HDFS dan menyimpan pengelompokan data di local path
    this.delete_folder_hdfs(path_HDFS+"/gkmc_output",hdfs)

    kanonymity_input.coalesce(1)
                    .write.format("parquet")
                    .save(path_HDFS+"/gkmc_output")

    gkmc_output.coalesce(1).
                write.
                option("header","true").
                option("sep",",").
                mode("overwrite").
                csv(output_path+"greedy-k-member-clustering")

    S.unpersist()
    json.unpersist()

  }

  def delete_folder_hdfs(pathName: String,hdfs:FileSystem) {
    val path = new Path(pathName)
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)
    }
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
