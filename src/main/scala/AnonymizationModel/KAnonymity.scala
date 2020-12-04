package AnonymizationModel

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class KAnonymity {

  def k_anonymity(spark: SparkSession,json:DataFrame,clusters: DataFrame, listBinaryTree:ListBuffer[BinaryTree],hdfs:FileSystem,path_HDFS:String,path_delete_function_HDFS:String): DataFrame ={
    import org.apache.spark.sql.functions._

    val clusters_schema:StructType = clusters.schema
    var clusters_temp:DataFrame = null
    var result: DataFrame = null

    var numClusters = clusters.select("Cluster").distinct().count().toInt
    this.delete_folder_hdfs(path_delete_function_HDFS+"/kanonymity1_tmp/",hdfs)
    clusters.coalesce(1).write.option("header", "true").
              csv(path_HDFS+"/kanonymity1_tmp/")

    // Perulangan untuk setiap cluster
    while(numClusters > 0){ //looping


      // Baca clusters dari file HDFS
      this.delete_folder_hdfs(path_delete_function_HDFS+"/kanonymity1/",hdfs)
      hdfs.rename(new Path(path_HDFS+"/kanonymity1_tmp"),
                  new Path(path_HDFS+"/kanonymity1"))
      clusters_temp = spark.read.format("csv").option("header", "true").schema(clusters_schema).
                      load(path_HDFS+"/kanonymity1/")


      // Mengambil sebuah cluster dari clusters
      val clusterName = "Cluster "+numClusters
      var clusterDF = clusters_temp.where(clusters_temp("Cluster").contains(clusterName))
      val clusterDF_temp = clusterDF

      this.delete_folder_hdfs(path_delete_function_HDFS+"/kanonymity2_tmp/",hdfs)
      clusterDF.coalesce(1).write.option("header", "true").
                csv(path_HDFS+"/kanonymity2_tmp/")

      // Melakukan iterasi untuk anonimisasi data
      var i = 1
      clusterDF.dtypes.foreach { element =>  // looping

        if(element._1 != "id" && element._1 != "Cluster" && !element._1.contains("_")) {

          if (element._2.contains("Integer")) {
            // Membaca file HDFS
            this.delete_folder_hdfs(path_delete_function_HDFS+"/kanonymity2/",hdfs)
            hdfs.rename(new Path(path_HDFS+"/kanonymity2_tmp"),
                        new Path(path_HDFS+"/kanonymity2"))
            clusterDF = spark.read.format("csv").option("header", "true").schema(clusters_schema).
                        load(path_HDFS+"/kanonymity2/")

            // Menulis ke file HDFS
            clusterDF = clusterDF.withColumn("Anonym_"+element._1,convert_to_numeric_anonymous(
                                  col("max_"+element._1),col("min_"+element._1)))
            clusterDF.coalesce(1).write.option("header", "true").
                      csv(path_HDFS+"/kanonymity2_tmp/")
//            clusterDistinctDF = clusterDistinctDF.withColumn("Anonym_"+element._1,convert_to_numeric_anonymous(col(element._1)))
            i += 1
          }
          else {
            val binaryTree = listBinaryTree(i)

            // Membaca file HDFS
            this.delete_folder_hdfs(path_delete_function_HDFS+"/kanonymity2/",hdfs)
            hdfs.rename(new Path(path_HDFS+"/kanonymity2_tmp"),
                        new Path(path_HDFS+"/kanonymity2"))
            clusterDF = spark.read.format("csv").option("header", "true").
                        load(path_HDFS+"/kanonymity2/")

            // Menulis file ke HDFS
            val distinctColumn = clusterDF.select(collect_set(element._1).as("Dist_"+element._1))
            if (binaryTree != null) {
              clusterDF = clusterDF.crossJoin(distinctColumn)
              clusterDF = clusterDF.withColumn("Anonym_"+element._1,
                          convert_to_category_anonymous(binaryTree)(col("Dist_"+element._1)) )
              clusterDF = clusterDF.drop("Dist_"+element._1)
              clusterDF.coalesce(1).write.option("header", "true").
                        csv(path_HDFS+"/kanonymity2_tmp/")
              i += 1
            }
            else {
              clusterDF = clusterDF.crossJoin(distinctColumn)
              clusterDF = clusterDF.withColumn("Anonym_"+element._1,
                          col("Dist_"+element._1).getItem(0) )
              clusterDF = clusterDF.drop("Dist_"+element._1)
              clusterDF.coalesce(1).write.option("header", "true").
                        csv(path_HDFS+"/kanonymity2_tmp/")
              i += 1
            }

          }

        }

      }

      // Membaca file HDFS
      this.delete_folder_hdfs(path_delete_function_HDFS+"/kanonymity2/",hdfs)
      hdfs.rename(new Path(path_HDFS+"/kanonymity2_tmp"),
                  new Path(path_HDFS+"/kanonymity2"))
      clusterDF = spark.read.format("csv").option("header", "true").
                  load(path_HDFS+"/kanonymity2/")

      // Mengambil nama column diawali dengan "Anonym"
      clusterDF = clusterDF.select(clusterDF.columns.filter(colName => colName.startsWith("Anonym")||
                  colName.contains("id")).map(clusterDF(_)): _*)

      // Menyimpan hasil anonimisasi
      if(result == null) {
        result = clusterDF
        this.delete_folder_hdfs(path_delete_function_HDFS+"/kanonymity3_tmp/",hdfs)
        result.coalesce(1).write.option("header", "true").
        csv(path_HDFS+"/kanonymity3_tmp/")
      }
      else{
        // Membaca result dari file HDFS
        this.delete_folder_hdfs(path_delete_function_HDFS+"/kanonymity3/",hdfs)
        hdfs.rename(new Path(path_HDFS+"/kanonymity3_tmp"),
                    new Path(path_HDFS+"/kanonymity3"))
        result = spark.read.format("csv").option("header", "true").
                  schema(result.schema).load(path_HDFS+"/kanonymity3/")

        // Menggabungkan result
        result = result.union(clusterDF)
        delete_folder_hdfs(path_delete_function_HDFS+"/kanonymity3_tmp/",hdfs)
        result.coalesce(1).write.option("header", "true").
                csv(path_HDFS+"/kanonymity3_tmp/")
      }

      // Menyimpan anonimisasi ke HDFS
      numClusters -= 1
      if(numClusters == 0){
        this.delete_folder_hdfs(path_delete_function_HDFS+"/kanonymity3/",hdfs)
        hdfs.rename(new Path(path_HDFS+"/kanonymity3_tmp"),
                    new Path(path_HDFS+"/kanonymity3"))
      }

      // Membuang cluster yang sudah pernah dianonimisasi
      clusters_temp = clusters_temp.except(clusterDF_temp).cache()
      clusters_temp.coalesce(1).write.option("header", "true").
                    csv(path_HDFS+"/kanonymity1_tmp/")
      if(numClusters == 0){
        this.delete_folder_hdfs(path_delete_function_HDFS+"/kanonymity1/",hdfs)
        hdfs.rename(new Path(path_HDFS+"/kanonymity1_tmp"), new Path(path_HDFS+"/kanonymity1"))
      }
    }

    this.delete_folder_hdfs(path_delete_function_HDFS+"/kanonymity2/",hdfs)
    hdfs.rename(new Path(path_HDFS+"/kanonymity3_tmp"),
                new Path(path_HDFS+"/kanonymity3"))
    result = spark.read.format("csv").option("header", "true").schema(result.schema).
                        load(path_HDFS+"/kanonymity3/")
    result = result.orderBy(asc("id"))

    return result
  }

  def convert_to_numeric_anonymous = udf ( (maxValue: Int, minValue: Int) => {
    "[" + minValue + "-" + maxValue + "]"
  })

//  def convert_to_numeric_anonymous = udf ( (listNum: Seq[Int]) => {
//    val maxValue = listNum.max
//    val minValue = listNum.min
//    "[" + minValue + "-" + maxValue + "]"
//  })

  def convert_to_category_anonymous(binaryTree: BinaryTree) = udf ( (listCategory: Seq[String]) => {
    if(listCategory.length > 1){
      binaryTree.root.name
    }
    else{
      listCategory.head.toString()
    }

  })

  def delete_folder_hdfs(pathName: String,hdfs:FileSystem) {
    val path = new Path(pathName)
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)
    }
  }

  def checkNumDistinct = udf ( (num: Int) => {
    if(num>1) true
    else false
  })

  def find_node_genaralization_with_level(selectedNode: Node, level: Int): Node = {
    if(selectedNode.level == level){
      return selectedNode
    }
    else{
      find_node_genaralization_with_level(selectedNode.parent,level)
    }
  }

  def list_of_all_attribute(spark: SparkSession, record: DataFrame): List[String] = {
    record.createOrReplaceTempView("tAdults")
    val test = spark.catalog.listColumns("tAdults").select("name","datatype")
    val df = test.select("name")
    val columnName = df.collect().map(_(0)).toList.asInstanceOf[List[String]]
    return columnName
  }

  def read_dgh_from_json(df: DataFrame,category: String): ListBuffer[Seq[String]] = {
    var result = ListBuffer[Seq[String]]()

    try{
      val tree = df.select("domain_generalization_hierarchy."+ category+".tree").collect()(0).getString(0)
      val dgh_json = df.select("domain_generalization_hierarchy." + category+".generalization")
      val dgh = dgh_json.collect()
      val dghArr = dgh.map(row => row.getSeq[Row](0))
      result += Seq[String](tree)
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

  def create_binary_tree_from_dgh_attribute(dgh: ListBuffer[Seq[String]]):BinaryTree = {
    val tree = new BinaryTree
    val queue = new mutable.Queue[Node]()
    var currentNode = new Node("",0)
    var initialize = true

    for(attribute <- dgh){
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
    return tree
  }

  def getNumPartitions(size:Int):Int = {
    var numPartitions = 1
    if(size >= 9)  numPartitions = size/9
    return numPartitions
  }

}
