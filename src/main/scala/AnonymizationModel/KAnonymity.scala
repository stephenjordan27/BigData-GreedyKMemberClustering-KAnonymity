package AnonymizationModel

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.lit

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class KAnonymity extends java.io.Serializable{

  def k_anonymity(spark: SparkSession,json:DataFrame,clusters: DataFrame): DataFrame ={
    var result: DataFrame = spark.emptyDataFrame

    var clusters_temp = clusters
    var numClusters = clusters.select("Cluster").distinct().count()
    val columnName = list_of_all_attribute(spark,clusters_temp)

    // Perulangan untuk setiap cluster
    while(numClusters > 0){
      try{
        val clusterName = clusters_temp.select("Cluster").first().getString(0)
        val clusterDF = clusters_temp.where(clusters_temp("Cluster").contains(clusterName))

        var clusterAnonymization: DataFrame = clusterDF.select("id")
        var recordDistinctValues = spark.emptyDataFrame
        var numDistinctValues = 0

        //////////////////////////////////////////////baris ini bermasalah////////////////////////////////////////////////
        // Perulangan untuk setiap kolom
        columnName.foreach { colName =>  // looping
          recordDistinctValues =  clusterDF.select(colName).distinct() // distinct record per 1 column
          recordDistinctValues.cache()
          numDistinctValues = recordDistinctValues.count().toInt

          if(colName != "id") {
            val columnValue = recordDistinctValues.first().get(0)

            if (numDistinctValues > 1 && columnValue.isInstanceOf[Int]) {
              val maxValue = recordDistinctValues.groupBy().max(colName).first().getInt(0)
              val minValue = recordDistinctValues.groupBy().min(colName).first().getInt(0)
              val generalizationNumeric = "[" + minValue + "-" + maxValue + "]"
              clusterAnonymization = clusterAnonymization.withColumn(colName, lit(generalizationNumeric))
            }
            else {
              val dgh = read_dgh_from_json(json, colName)

              if (numDistinctValues > 1 && columnValue.isInstanceOf[String] && dgh != null) {
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
          recordDistinctValues.unpersist()
        }
        //////////////////////////////////////////////baris ini bermasalah////////////////////////////////////////////////

        if(result.isEmpty) result = clusterAnonymization
        else result = result.union(clusterAnonymization)
        clusters_temp = clusters_temp.except(clusterDF)
        numClusters -= 1
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
      val dgh_json = df.select("domain_generalization_hierarchy." + category)
      val dgh = dgh_json.collect()
      val dghArr = dgh.map(row => row.getSeq[Row](0))
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

}
