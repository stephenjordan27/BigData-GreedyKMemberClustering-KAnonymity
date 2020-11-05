package AnonymizationModel

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{lit, rand, udf}
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class GreedyKMemberClustering {


  
  def greedy_k_member_clustering(spark: SparkSession, sc: SparkContext, json: DataFrame, S: DataFrame, k: Int, listDataType: Array[DataType]): DataFrame = {
    var S_size = S.count().toInt

    if(S_size <= k){
      return S
    }

    // Melakukan inisialisasi keseluruhan
    var S_temp = S
    val schema = S_temp.schema
    var clusters: DataFrame = null
    var r = S.orderBy(rand()).limit(1) //query
    val listColumnName = list_of_all_attribute(spark,S) // list nilai kategorikal r

    // Melakukan inisialisasi while (S_temp.count() >= k)
    var c:DataFrame = null
    var cluster_c:DataFrame = null
    var cluster_name:String = ""
    var index = 1

    val informationLossUDF = udf {
      // Parameter UDF
      (
        cluster: DataFrame, clusterSize: Int, listColumnName: List[String],
        listDataType: Array[DataType],listNumDistinctValues: ListBuffer[Int],
        listCategoricalDistinctValues:ListBuffer[Array[Row]]
      ) =>
      // Calling Function
      calculate_information_loss(spark,json,cluster,clusterSize,listColumnName,
        listDataType, listNumDistinctValues,listCategoricalDistinctValues)
    }


    spark.udf.register("informationLossUDF", informationLossUDF)



    // Apabila jumlah data >= k, maka lakukan perulangan
    while (S_size >= k){
      r = S.orderBy(rand()).limit(1)

      // Mencari record tabel S terjauh dengan record r
      r = furthest_record_from_r(spark,S_temp,r,json) // 5 detik

      // Membuang record r dari tabel S (1)
      S_temp = S_temp.except(r)
      S_size -= 1

      // Membuat penamaan sebuah cluster
      cluster_name = "Cluster "+ index
      index += 1

      // Membuat record r terjauh sebagai centroid cluster baru (c)
      c = r
      var c_size = 1
      val c_temp = c // without Cluster,Info
      var r_temp = r // without Cluster,Info
      cluster_c = c.withColumn("Cluster",lit(cluster_name))
      cluster_c = cluster_c.withColumn("Info",lit("Centroid"))



      // Mencari kelompok data pada cluster terdekat (c)
      while ( c_size < k ){

        // Menghitung nilai unik masing-masing kolom
        var listNumDistinctValues = new ListBuffer[Int]
        var listCategoricalDistinctValues = new ListBuffer[Array[Row]]
        val listColumnName = list_of_all_attribute(spark,c)
        listColumnName.foreach{ colName =>
          val distinctValues = c.select(colName).distinct().cache()
          listNumDistinctValues += distinctValues.count().toInt
          listCategoricalDistinctValues += distinctValues.collect()
          distinctValues.unpersist()
        }


        // Mencari record terbaik sebagai anggota cluster (c)

        r = find_best_record(spark,sc,json,S_temp,c_temp,listColumnName,S_size,c_size,listDataType,listNumDistinctValues,listCategoricalDistinctValues)

        // Mengelompokan data terhadap c -> find best record
        c = c.union(r)
        c_size += 1

        // Membuang record r dari tabel S (2)
        S_temp = S_temp.except(r)
        S_size -= 1

        // Menambahan kolom nama cluster dan info pada record r
        r_temp = r.withColumn("Cluster",lit(cluster_name))
        r_temp = r_temp.withColumn("Info",lit("Member"))

        // Mengelompokan data terhadap cluster_c (nama cluster, info) -> output pengelompokan data
        cluster_c = cluster_c.union(r_temp)
        r = r

      }

      // Mengatasi error union, jika result == null
      if(clusters == null) clusters = cluster_c
      else clusters = clusters.union(cluster_c)
    }

    // Jika S_temp masih ada sisa (masih ada data yang belum di kelompokan)
    var clusterName = ""
    val listClusterName = clusters.select("Cluster").distinct().collect()
    var remainingRecord: DataFrame = null // record with Cluster Name only
    var record: DataFrame = null

    while (S_size > 0){

      // Mengambil record yang tersisa secara acak
      r = S_temp.orderBy(rand()).limit(1).cache()

      // Membuang record r dari tabel S (3)
      S_temp = S_temp.except(r)
      S_size -= 1

      // Mencari cluster terbaik (c) untuk anggota record r
      c = find_best_cluster(spark,json,clusters,r,listClusterName,listColumnName,k,listDataType)
      clusterName = c.select("Cluster").first().getString(0)

      // Menampung hasil pengelompokan data
      if(remainingRecord == null) {
        remainingRecord = r.withColumn("Cluster",lit(clusterName))
      }
      else {
        record = r.withColumn("Cluster",lit(clusterName))
        remainingRecord = remainingRecord.union(record).distinct()
      }

      r.unpersist()
    }

    clusters = clusters.drop("Info")
    if(remainingRecord != null) clusters = clusters.union(remainingRecord)
    return clusters
  }

  def list_of_all_attribute(spark: SparkSession, record: DataFrame): List[String] = {
    record.createOrReplaceTempView("tAdults")
    val test = spark.catalog.listColumns("tAdults").select("name","datatype")
    val df = test.select("name")
    val columnName = df.collect().map(_(0)).toList.asInstanceOf[List[String]]
    return columnName
  }

  def find_best_record(spark: SparkSession, sc: SparkContext, json: DataFrame, S: DataFrame, c: DataFrame, listColumnName: List[String], S_size: Int, c_size: Int, listDataType: Array[DataType], listNumDistinctValues: ListBuffer[Int], listCategoricalDistinctValues:ListBuffer[Array[Row]]): DataFrame = {
    var best:DataFrame = null
    var min:Double = Int.MaxValue
    var unclustered_record_size = S_size
    var unclustered_record = S

    while(unclustered_record_size > 0){
      val r = unclustered_record.limit(1)
      val diff = calculate_substraction_information_loss(spark,json,c,r,listColumnName,c_size, listDataType,listNumDistinctValues, listCategoricalDistinctValues) // 38 detik - 1 iterasi                 //error
      if(diff < min){
        min = diff
        best = r
      }
      unclustered_record_size -= 1
      unclustered_record = unclustered_record.except(r)
    }
    return best
  }

  def find_best_cluster(spark: SparkSession, json: DataFrame, clusters: DataFrame, r: DataFrame, listClusterName: Array[Row], listColumnName: List[String], c_size: Int, listDataType: Array[DataType]): DataFrame = {
    // inisialisasi variabel
    var min:Double = Int.MaxValue
    var best:DataFrame = null

    // Untuk seluruh baris data, lakukan perulangan berikut
    listClusterName.foreach{ row => // setiap cluster
      val clusterName = row.getString(0)
      val c = clusters.where(clusters("Cluster").contains(clusterName)) // cluster i
      val c_temp = c.drop("Cluster","Info")

      // Menghitung nilai unik masing-masing kolom
      var listNumDistinctValues = new ListBuffer[Int]
      var listCategoricalDistinctValues = new ListBuffer[Array[Row]]
      val listColumnName = list_of_all_attribute(spark,c_temp)

      listColumnName.foreach{ colName =>
        val distinctValues = c.select(colName).distinct()
        val numDistincetValues = distinctValues.count().toInt
        listNumDistinctValues += numDistincetValues
        listCategoricalDistinctValues += distinctValues.collect()
      }

      // Menghitung selisih information loss
      val diff = calculate_substraction_information_loss(spark,json,c_temp,r,listColumnName,c_size, listDataType,listNumDistinctValues, listCategoricalDistinctValues)
      if (diff < min) {
        min = diff
        best = c
      }
    }
    return best
  }


  // https://stackoverflow.com/questions/39355149/how-to-read-json-with-schema-in-spark-dataframes-spark-sql
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

  def furthest_record_from_r(spark: SparkSession, S: DataFrame, r: DataFrame, json: DataFrame):DataFrame = {
    var best = -1
    var min_record_distance:Double = Integer.MAX_VALUE
    val listColumnName = list_of_all_attribute(spark,r)
    val r_values = r.first().toSeq

    S.collect().foreach { row => // looping baris
      // record distance
      var record_distance:Double = 0.0
      val record_id = row(0).toString.toInt

      //////////////////////////////////////////////baris ini diganti//////////////////////////////////////////////////
      listColumnName.zipWithIndex.foreach { case(colName,i) =>  // looping kolom
        if(colName != "id") {
          // Menghitung distance numerik
          try{
            if (row(i).isInstanceOf[Int]) {
              val num1 = row(i).toString.toInt
              val num2 = r_values(i).toString.toInt
              val max = S.groupBy().max(colName).first().getInt(0) // query
              val min = S.groupBy().min(colName).first().getInt(0) // query
              record_distance += calculate_numeric_distance(num1, num2, max, min)
            }
            // Menghitung distance kategorikal

            else {
              val dgh = read_dgh_from_json(json, colName)
              if(dgh != null){
                val category1 = row(i).toString
                val category2 = r_values(i).toString
                record_distance += calculate_categorical_distance(dgh, category1, category2)
              }
            }
          }
          catch {
            case x: Exception => {
              print()
            }
          }

        }
      }
      //////////////////////////////////////////////baris ini diganti//////////////////////////////////////////////////
      if(record_distance < min_record_distance){
        best = record_id
        min_record_distance = record_distance
      }

    }

    val result = S.where("id == "+best)
    return result

  }

  def calculate_numeric_distance(num1: Int, num2: Int, max:Int, min:Int): Double = {
    try{
      val result:Double =  Math.abs(num1-num2)/Math.abs(max-min)
      return BigDecimal(result).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    }
    catch {
      // Case statement
      case x: Exception => {
        return 0.0
      }
    }
  }

  def calculate_numeric_distance_information_loss(max_num: Int, min_num: Int, cluster_size: Int): Double = {
    var result:Double = 0.0
    try{
      result = Math.abs(max_num-min_num)/cluster_size
      result = BigDecimal(result).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      return result
    }
    catch {
      case x: Exception => {
        return 0.0
      }
    }
  }

  def calculate_categorical_distance(dgh:ListBuffer[Seq[String]],category1: String, category2: String): Double = {
    var result:Double = 0.0

    val binaryTree = create_binary_tree_from_dgh_attribute(dgh)
    val node1 = binaryTree.search(category1)
    val node2 = binaryTree.search(category2)

    if(node1 != null && node2 != null){
      val LCA = new LowestCommonAncestor()
      val LCA_root_name = LCA.findLCA(binaryTree.root, node1.name, node2.name)
      val H_subtree = binaryTree.search(LCA_root_name).level
      val H_TD = binaryTree.getHeight(binaryTree.root).toDouble
      result = H_subtree / H_TD
    }
    result = BigDecimal(result).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    return result
  }

  def calculate_categorical_distance_information_loss(dgh:ListBuffer[Seq[String]]): Double = {
    var result:Double = 0.0
    val binaryTree = create_binary_tree_from_dgh_attribute(dgh)
    val H_subtree = 1.0
    val H_TD = binaryTree.getHeight(binaryTree.root)
    result = H_subtree / H_TD
    result = BigDecimal(result).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    return result
  }

  def calculate_record_distance(numeric_distance:Double,categorical_distance:Double): Double ={
    return numeric_distance+categorical_distance
  }

  def calculate_substraction_information_loss(spark: SparkSession, json: DataFrame, c: DataFrame, r : DataFrame, listColumnName: List[String], c_size:Int, listDataType: Array[DataType], listNumDistinctValues: ListBuffer[Int], listCategoricalDistinctValues:ListBuffer[Array[Row]]): Double={
    var information_loss_c = 0.0
    try{
      val information_loss_c_union_r = calculate_information_loss(spark,json,c.union(r),c_size,listColumnName,listDataType,listNumDistinctValues,listCategoricalDistinctValues)
    }
    catch {
      case x: Exception => {
        print("")
      }
    }
    val information_loss_c_union_r = calculate_information_loss(spark,json,c.union(r),c_size,listColumnName,listDataType,listNumDistinctValues,listCategoricalDistinctValues)
    if(c_size > 1){
      information_loss_c = calculate_information_loss(spark,json,c,c_size,listColumnName,listDataType,listNumDistinctValues,listCategoricalDistinctValues)
    }
    val substraction_information_loss = information_loss_c_union_r - information_loss_c
    return substraction_information_loss
  }

  def calculate_information_loss(spark: SparkSession, json: DataFrame,  cluster: DataFrame, clusterSize: Int, listColumnName: List[String], listDataType: Array[DataType],listNumDistinctValues: ListBuffer[Int],listCategoricalDistinctValues:ListBuffer[Array[Row]]): Double = {
      var information_loss:Double = 0.0

      //////////////////////////////////////////////baris ini bermasalah//////////////////////////////////////////////////
      listColumnName.zipWithIndex.foreach { case(colName,i) =>  // looping                                       //error
        if(colName != "id") {

          // Menghitung distance numerik information loss
          try{
            if(listDataType(i).isInstanceOf[IntegerType]){
                val max_num = cluster.groupBy().max(colName).first().getInt(0) // query
                val min_num = cluster.groupBy().min(colName).first().getInt(0) // query
                information_loss += calculate_numeric_distance_information_loss(max_num,min_num,clusterSize)
            }
            // Menghitung distance kategorikal
            else {
              val dgh = read_dgh_from_json(json, colName)

              if(dgh != null) {

                // Menjaga Lowest Common Ancestor hanya dilakukan jika 2 values unik saja
                if (listNumDistinctValues(i) > 2) {
                  information_loss += calculate_categorical_distance_information_loss(dgh)
                }
                else if(listNumDistinctValues(i) == 2){
                  val category1 = listCategoricalDistinctValues(i)(0).getString(0)
                  val category2 = listCategoricalDistinctValues(i)(1).getString(0)
                  information_loss += calculate_categorical_distance(dgh,category1,category2)
                }
                else{
                  information_loss += 0
                }

              }

            }
          }
          catch {
            case x: Exception => {
              print("")
            }
          }



        }

      }
      //////////////////////////////////////////////baris ini bermasalah////////////////////////////////////////////////
      return clusterSize*information_loss

  }

}
