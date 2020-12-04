package AnonymizationModel

import AnonymizationModel.MainClusterization.create_list_binary_tree_attribute
import org.apache.spark.sql.{DataFrame, SparkSession}

object MainAnonymization {

  //    val path_HDFS_CLUSTER= "hdfs://master:9070/skripsi-jordan/temp"
  //    val path_HDFS_LOCAL= "hdfs://localhost:50071/skripsi"
  //    val path_delete_HDFS_CLUSTER= "/skripsi-jordan/temp"
  //    val path_delete_HDFS_LOCAL= "/skripsi"

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("K-Anonymity")
      .getOrCreate()

    // Parameter JSON
    val path_JSON = args(0)
    val json:DataFrame = spark.read.option("multiline", "true").json(path_JSON).cache() // ganti di sini

    // Parameter Greedy K-Member Clustering
    val hdfs_name = json.select("hdfs").first().getString(0)
    val temp_files = json.select("temp_files").first().getString(0)
    val output_path = json.select("output_path").first().getString(0)

    // Inisialisasi path HDFS untuk membaca csv dan delete folder
    val path_HDFS = hdfs_name + temp_files
    val path_delete_function_HDFS = temp_files

    // Membaca input k-anonymity dari HDFS
    val clusters = spark.read.parquet(path_HDFS+"/gkmc_output/")
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:50071/"), hadoopConf)

    // Melakukan anonimisasi pada data yang telah dikelompokan menggunakan k-anonymity
    val KAnonymity = new KAnonymity()
    val x = clusters.select(clusters.columns.filter(colName => !colName.contains("_")).map(clusters(_)): _*)
    val listBinaryTree = create_list_binary_tree_attribute(x,json)
    val kanonymityDF = KAnonymity.k_anonymity(spark,json,clusters,listBinaryTree,hdfs,path_HDFS,path_delete_function_HDFS)

    // Menyimpan hasil pengelompokan data ke dalam CSV
    kanonymityDF.coalesce(1)
                .write
                .option("header","true")
                .option("sep",",")
                .mode("overwrite")
                .csv(output_path+"k-anonymity")
    
  }

}
