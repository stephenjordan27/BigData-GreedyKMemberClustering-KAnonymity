package AnonymizationModel

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, countDistinct, lit}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class KAnonymity extends java.io.Serializable{

  def k_anonymity(spark: SparkSession,json:DataFrame,clusters: DataFrame, listDataType: Array[DataType]): DataFrame ={
    var result: DataFrame = spark.emptyDataFrame

    var clusters_temp = clusters
    var numClusters = clusters.select("Cluster").distinct().count().toInt
    val columnName = list_of_all_attribute(spark,clusters_temp)

    // Perulangan untuk setiap cluster
    while(numClusters > 0){
      try{
        val clusterName = clusters_temp.select("Cluster").first().getString(0)
        val clusterDF = clusters_temp.where(clusters_temp("Cluster").contains(clusterName))

        var clusterAnonymization: DataFrame = clusterDF.select("id")


        //////////////////////////////////////////////baris ini bermasalah////////////////////////////////////////////////
        // Perulangan untuk setiap kolom

        val listNumDistinctValuesCluster = clusterDF.select(clusterDF.columns.map(c =>
                                           countDistinct(col(c)).alias(c)): _*).first().toSeq

        columnName.zipWithIndex.foreach { case(colName,i) =>  // looping

          if(colName != "id") {
            if (listNumDistinctValuesCluster(i).toString.toInt > 1 && listDataType(i).isInstanceOf[IntegerType]) {
              val maxValue = clusterDF.groupBy().max(colName).first().getInt(0)
              val minValue = clusterDF.groupBy().min(colName).first().getInt(0)
              val generalizationNumeric = "[" + minValue + "-" + maxValue + "]"
              clusterAnonymization = clusterAnonymization.withColumn(colName, lit(generalizationNumeric))
            }
            else {
              val dgh = read_dgh_from_json(json, colName)

              if (listNumDistinctValuesCluster(i).toString.toInt > 1 && listDataType(i).isInstanceOf[StringType] && dgh != null) {
                val binaryTree = create_binary_tree_from_dgh_attribute(dgh)
                val generalizationCategorical = binaryTree.root.name // bagian anonimisasi
                clusterAnonymization = clusterAnonymization.withColumn(colName, lit(generalizationCategorical))
              }
              else {
                val column = clusterDF.select("id", colName).withColumnRenamed("id", "id_temp")
                clusterAnonymization = clusterAnonymization.join(column, clusterAnonymization("id") === column("id_temp"))
                clusterAnonymization = clusterAnonymization.drop("id_temp")
              }

            }

          }

        }
        //////////////////////////////////////////////baris ini bermasalah////////////////////////////////////////////////

        if(result.isEmpty) result = clusterAnonymization
        else result = result.union(clusterAnonymization)

        numClusters -= 1
        clusters_temp = clusters_temp.except(clusterDF).repartition(getNumPartitions(numClusters)).cache()


      }
      catch {
        case x: Exception => {
          print("")
        }
      }

    }
    result = result.drop("Cluster")

    import org.apache.spark.sql.functions._
    result = result.orderBy(asc("id"))

    return result
  }

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
