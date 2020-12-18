package AnonymizationModel

import AnonymizationModel.MainClusterization.create_list_binary_tree_attribute
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object MainAnonymization {

  //    val path_HDFS_CLUSTER= "hdfs://master:9070/skripsi-jordan/temp"
  //    val path_HDFS_LOCAL= "hdfs://localhost:50071/skripsi"
  //    val path_delete_HDFS_CLUSTER= "/skripsi-jordan/temp"
  //    val path_delete_HDFS_LOCAL= "/skripsi"

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("yarn")
      .appName("K-Anonymity")
      .getOrCreate()

    // Parameter JSON
    val path_JSON = args(0)
    val json:DataFrame = spark.read.option("multiline", "true").json(path_JSON).cache() // ganti di sini
    val sensitive_identifier_json:ListBuffer[Seq[String]] = read_element_from_json(json,"sensitive_identifier")

    // Parameter Greedy K-Member Clustering
    val hdfs_name = json.select("hdfs").first().getString(0)
    val temp_files = json.select("temp_files").first().getString(0)
    val output_path = json.select("output_path").first().getString(0)
    val sensitive_identifier = get_attribute_name_json(sensitive_identifier_json)(0)

    // Inisialisasi path HDFS untuk membaca csv dan delete folder
    val path_HDFS = hdfs_name + temp_files
    val path_delete_function_HDFS = temp_files

    // Membaca input k-anonymity dari HDFS
    val clusters = spark.read.parquet(path_HDFS+"/gkmc_output/")
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfs_name), hadoopConf)

    // Melakukan anonimisasi pada data yang telah dikelompokan menggunakan k-anonymity
    val KAnonymity = new KAnonymity()
    val GKMC =  clusters.select(clusters.columns.
                filter(colName => !colName.startsWith("max_") &&
                !colName.contains("min_")).map(clusters(_)) : _*)
    val listBinaryTree = create_list_binary_tree_attribute(GKMC,json)
    val kanonymityDF = KAnonymity.k_anonymity(spark,json,clusters,listBinaryTree,hdfs,path_HDFS,path_delete_function_HDFS,sensitive_identifier)

    // Menyimpan hasil pengelompokan data ke dalam CSV
    kanonymityDF.coalesce(1)
                .write
                .option("header","true")
                .option("sep",",")
                .mode("overwrite")
                .csv(output_path+"k-anonymity")

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
