package AnonymizationModel

import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class KAnonymity extends java.io.Serializable{

  def k_anonymity(spark: SparkSession,json:DataFrame,clusters: DataFrame, listDataType:Array[DataType] ): DataFrame ={
    var result: DataFrame = null
    var clusters_temp = clusters.cache()
    var clusters_temp_size = clusters_temp.count()


    val listColumnName = list_of_all_attribute(spark,clusters.drop("id","Cluster"))

    // Perulangan untuk setiap cluster
    while(clusters_temp_size != 0){
      spark.sqlContext.clearCache()
      val sample = clusters_temp.limit(1)
      val clusterName = sample.select("Cluster").first().getString(0)
      val clusterDF = clusters_temp.where(clusters("Cluster").contains(clusterName))
      val clusterWithoutID_DF = clusterDF.drop("id","Cluster")
      val cluster_size = clusterDF.count().toInt

      var clusterAnonymization: DataFrame = clusterDF.select("id")

      var numPartitions = 1
      if(cluster_size >= 9){
        numPartitions = cluster_size/9
      }



      val maxDF = clusterWithoutID_DF.groupBy().max().head()
      val minDF = clusterWithoutID_DF.groupBy().min().head()

      val listColumnMaxValue = maxDF.toSeq
      val listColumnMinValue = minDF.toSeq

      import org.apache.spark.sql.functions.countDistinct
      import org.apache.spark.sql.functions.col
      val listDistinctValue = clusterWithoutID_DF.select(clusterWithoutID_DF.columns.map(c =>
                              countDistinct(col(c)).alias(c)): _*).first().toSeq

//      val listColumnMaxValue = new ListBuffer[Int]()
//      val listColumnMinValue = new ListBuffer[Int]()
//      val listDistinctValue = new ListBuffer[Int]()

//      clusters.createOrReplaceTempView("tCluster")
//
//      listColumnName.zipWithIndex.foreach { case (colName, i) =>
//        if(colName != "id"){
//          if(listDataType(i).isInstanceOf[IntegerType]){
//            listColumnMaxValue += spark.sql("SELECT max("+colName+") FROM tCluster").first().getInt(0)
//            listColumnMinValue += spark.sql("SELECT min("+colName+") FROM tCluster").first().getInt(0)
//          }
//          else{
//            listColumnMaxValue += 0
//            listColumnMinValue += 0
//          }
//          listDistinctValue += spark.sql("SELECT distinct("+colName+") FROM tCluster").count().toInt
//        }
//      }
//
//      spark.catalog.dropTempView("tCluster")

      import org.apache.spark.sql.functions._

      //////////////////////////////////////////////baris ini bermasalah////////////////////////////////////////////////

      var initialize = true

      listColumnName.zipWithIndex.foreach { case(colName,i) =>  // looping

        val dgh = read_dgh_from_json(json, colName)

        if(colName != "id") {

          if (listDistinctValue(i).toString.toInt > 1 && listDataType(i).isInstanceOf[IntegerType]) {
            val maxValue = listColumnMaxValue(i)
            val minValue = listColumnMinValue(i)
            val generalizationNumeric = "[" + minValue + "-" + maxValue + "]"
            clusterAnonymization = clusterAnonymization.withColumn(colName, lit(generalizationNumeric))
          }
          else if(listDistinctValue(i).toString.toInt > 1 && listDataType(i).isInstanceOf[StringType] && dgh!=null) {
            val treeName = dgh.remove(0)(0)
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


      //////////////////////////////////////////////baris ini bermasalah////////////////////////////////////////////////

      if(initialize) {
        result = clusterAnonymization
        initialize = false
      }
      else result = result.union(clusterAnonymization)

      clusters_temp = clusters_temp.except(clusterDF)
      clusters_temp_size -= cluster_size

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

}
