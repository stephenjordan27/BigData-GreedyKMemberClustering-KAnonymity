package AnonymizationModel

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class KAnonymity {

  def convert_to_numeric_anonymous = udf ( (listNum: Seq[Int]) => {
    val maxValue = listNum.max
    val minValue = listNum.min
    "[" + minValue + "-" + maxValue + "]"
  })

  def convert_to_category_anonymous(binaryTree: BinaryTree) = udf ( (listCategory: Seq[String]) => {
    if(listCategory.length > 1){
      binaryTree.root.name
    }
    else{
      listCategory.head.toString()
    }

  })

  def k_anonymity(spark: SparkSession,json:DataFrame,clusters: DataFrame, listDataType: Array[DataType], listBinaryTree:ListBuffer[BinaryTree]): DataFrame ={
    import org.apache.spark.sql.functions._

    var clusters_temp = clusters
    var result: DataFrame = null
    var numClusters = clusters.select("Cluster").distinct().count().toInt

    // Perulangan untuk setiap cluster
    while(numClusters > 0){
      try{
        val clusterName = "Cluster "+numClusters
        val clusterDF = clusters_temp.where(clusters_temp("Cluster").contains(clusterName))
        var clusterDistinctDF = clusterDF.select(clusterDF.columns.map(c => collect_set(col(c)).alias(c) ): _*)
        //////////////////////////////////////////////baris ini bermasalah////////////////////////////////////////////////

        var i = 1
        clusterDF.dtypes.foreach { element =>  // looping

          if(element._1 != "id" && element._1 != "Cluster") {

            if (element._2.contains("Integer")) {
              clusterDistinctDF = clusterDistinctDF.withColumn("Anonym_"+element._1,convert_to_numeric_anonymous(col(element._1)))
              i += 1
            }
            else {
              val binaryTree = listBinaryTree(i)
              if (binaryTree != null) {
                clusterDistinctDF = clusterDistinctDF.withColumn("Anonym_"+element._1,convert_to_category_anonymous(binaryTree)(col(element._1)) )
                i += 1
              }
              else {
                clusterDistinctDF = clusterDistinctDF.withColumn("Anonym_"+element._1,col(element._1).getItem(0) )
                i += 1
              }

            }

          }

        }

        clusterDistinctDF = clusterDistinctDF.select(clusterDistinctDF.columns.filter(colName => colName.startsWith("Anonym")||colName.contains("id")).map(clusterDistinctDF(_)): _*)
        clusterDistinctDF = clusterDistinctDF.withColumn("id_temp",explode(col("id"))).drop("id")
        //////////////////////////////////////////////baris ini bermasalah////////////////////////////////////////////////

        if(result == null) result = clusterDistinctDF
        else result = result.union(clusterDistinctDF).repartition(1)

        numClusters -= 1
        clusters_temp = clusters_temp.except(clusterDF).repartition(1)
      }
      catch {
        case x: Exception => {
          print("")
        }
      }

    }

    import org.apache.spark.sql.functions._
    spark.sqlContext.clearCache()
    result = result.orderBy(asc("id_temp")).drop("id")

    return result
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
