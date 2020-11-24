package AnonymizationModel

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{max, _}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

class GreedyKMemberClustering extends Serializable {

  def getNumPartitions(size:Int):Int = {
    var numPartitions = 1
    if(size >= 10)  numPartitions = size/10
    return numPartitions
  }

  def calculateNumericalDistance = udf ( (num1: Int,num2: Int, max_num:Int, min_num:Int) => {
    Math.abs(num1-num2)*1.0/Math.abs(max_num-min_num)
  })

  def calculateNumericalInformationLoss(cluster_size:Int) = udf ( (max_num: Int,min_num: Int) => {
    Math.abs(max_num-min_num)*1.0/cluster_size
  })

  def calculateNumericalInformationLossUnion(cluster_size:Int) = udf ( (max_c:Int, min_c:Int, num:Int) => {
    var max_value = max_c
    var min_value = min_c
    if(num > max_value){
      max_value = num
    }
    if(num < min_value){
      min_value = num
    }
    Math.abs(max_value-min_value)*1.0/cluster_size
  })

  def calculateCategoricalDistance(binaryTree: BinaryTree):UserDefinedFunction = udf( (category1: String, category2: String) => {
    if(binaryTree!=null){
      val node1 = binaryTree.search(category1)
      val node2 = binaryTree.search(category2)

      if(node1 != null && node2 != null && node1.name != node2.name){
        val LCA = new LowestCommonAncestor()
        val LCA_root_name = LCA.findLCA(binaryTree.root, node1.name, node2.name)
        val H_subtree = binaryTree.search(LCA_root_name).level
        val H_TD = binaryTree.getHeight(binaryTree.root).toDouble
        BigDecimal(H_subtree*1.0 / H_TD).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      }
      else{
        0.0
      }

    }
    else{
      0.0
    }

  })

  def calculateCategoricalInformationLoss(binaryTree: BinaryTree):UserDefinedFunction = udf( (listDistinctValues:Seq[Any]) => {
    if(binaryTree!=null){

      val numDistinctValues = listDistinctValues.length

      if(numDistinctValues == 2){
        val node1 = binaryTree.search(listDistinctValues(0).toString)
        val node2 = binaryTree.search(listDistinctValues(1).toString)
        if(node1 != null && node2 != null){
          val LCA = new LowestCommonAncestor()
          val LCA_root_name = LCA.findLCA(binaryTree.root, node1.name, node2.name)
          val H_subtree = binaryTree.search(LCA_root_name).level
          val H_TD = binaryTree.getHeight(binaryTree.root).toDouble
          BigDecimal(H_subtree*1.0 / H_TD).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        }
        else{
          0.0
        }
      }
      else if(numDistinctValues > 2){
        val H_subtree = 1
        val H_TD = binaryTree.getHeight(binaryTree.root).toDouble
        BigDecimal(H_subtree*1.0 / H_TD).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      }
      else{
        0.0
      }

    }
    else{
      0.0
    }

  })

  def calculateCategoricalInformationLossUnion(binaryTree: BinaryTree):UserDefinedFunction = udf( (check:Boolean,listCategory:Array[String]) => {
    if(binaryTree!=null){

      var numDistinctValues = 0
      if(check){
        numDistinctValues = listCategory.length
      }
      else{
        numDistinctValues = listCategory.length+1
      }


      if(numDistinctValues == 2){
        val node1 = binaryTree.search(listCategory(0).toString)
        val node2 = binaryTree.search(listCategory(1).toString)
        if(node1 != null && node2 != null){
          val LCA = new LowestCommonAncestor()
          val LCA_root_name = LCA.findLCA(binaryTree.root, node1.name, node2.name)
          val H_subtree = binaryTree.search(LCA_root_name).level
          val H_TD = binaryTree.getHeight(binaryTree.root).toDouble
          BigDecimal(H_subtree*1.0 / H_TD).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        }
        else{
          0.0
        }

      }
      else if(numDistinctValues > 2){
        val H_subtree = 1
        val H_TD = binaryTree.getHeight(binaryTree.root).toDouble
        BigDecimal(H_subtree*1.0 / H_TD).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      }
      else{
        0.0
      }

    }
    else{
      0.0
    }

  })

  def sum_(cols: Column*) = cols.foldLeft(lit(0))(_ + _)

  def furthest_record_from_r_optimize(json: DataFrame, S: DataFrame, r:DataFrame, listBinaryTree:ListBuffer[BinaryTree]): DataFrame = {
    val listColumnName = S.columns.toSeq
    val r_temp = r.select(r.columns.map(c => col(c).alias(c+"_r")): _*)
    var S_temp = S


    S_temp =  S_temp.crossJoin(r_temp)
    var i = 1

    S_temp.dtypes.foreach(element =>
      if(!element._1.contains("_r") && !element._1.contains("id")){
        if(element._2.contains("Integer")){
          val max_value = S_temp.select(max(element._1).as("max_"+element._1))
          val min_value = S_temp.select(min(element._1).as("min_"+element._1))
          S_temp = S_temp.crossJoin(max_value)
          S_temp = S_temp.crossJoin(min_value)
          S_temp = S_temp.withColumn("Dist_"+element._1,calculateNumericalDistance(col(element._1),col(element._1+"_r"),col("max_"+element._1),col("min_"+element._1) ) )
          i += 1
        }
        else{
          S_temp = S_temp.withColumn("Dist_"+element._1,calculateCategoricalDistance(listBinaryTree(i))( S_temp(element._1),S_temp(element._1+"_r") ))
          i += 1
        }

      }
    )

    val distance = S_temp.select(S_temp.columns.filter(_.startsWith("Dist_")).map(S_temp(_)) : _*)
    val columnstosum = distance.columns.toSeq.map(col _)

    S_temp = S_temp.withColumn("DistRecord",sum_(columnstosum: _*))
    S_temp = S_temp.orderBy(desc("DistRecord"))

    val furthest_record_from_r = S_temp.select(listColumnName.head, listColumnName.tail: _*).limit(1)

    return furthest_record_from_r
  }

  def calculate_substraction_information_loss_optimize(json: DataFrame, unionDF: DataFrame, c: DataFrame, cluster_size:Int): DataFrame = {
    var cluster_temp = unionDF

    unionDF.dtypes.filter(!_._1.contains("_")).foreach(element =>
      if(!element._1.contains("id") && !element._1.contains("Cluster") && !element._1.contains("Info")){
        if(element._2.contains("Integer")){
          cluster_temp = cluster_temp.withColumn("ILcluster_" + element._1, calculateNumericalInformationLoss(cluster_size)(col("max_"+element._1),col("min_"+element._1)))
          cluster_temp = cluster_temp.withColumn("ILunion_" + element._1, calculateNumericalInformationLossUnion(cluster_size)(col("max_"+element._1),col("min_"+element._1),col(element._1)))
        }
        else{
          cluster_temp = cluster_temp.withColumn("ILcluster_" + element._1, lit(1))
          cluster_temp = cluster_temp.withColumn("ILunion_" + element._1, lit(1))
        }
      }
    )
    val infoloss_cluster = cluster_temp.select(cluster_temp.columns.filter(_.contains("ILcluster_")).map(cluster_temp(_)): _*)
    val sum_infoloss_cluster = infoloss_cluster.columns.toSeq.map(col _)

    val infoloss_union = cluster_temp.select(cluster_temp.columns.filter(_.contains("ILunion_")).map(cluster_temp(_)): _*)
    val sum_infoloss_union = infoloss_union.columns.toSeq.map(col _)

    cluster_temp = cluster_temp.withColumn("Total_ILcluster",sum_(sum_infoloss_cluster: _*)*cluster_size)
    cluster_temp = cluster_temp.withColumn("Total_ILunion",sum_(sum_infoloss_union: _*)*cluster_size)

    cluster_temp = cluster_temp.withColumn("Subs_IL",col("Total_ILunion")-col("Total_ILcluster"))
    cluster_temp = cluster_temp.drop(cluster_temp.columns.filter(_.startsWith("IL")): _*)

    return cluster_temp

  }

  
  def greedy_k_member_clustering(spark: SparkSession, json: DataFrame, S: DataFrame, k: Int, listDataType: Array[DataType], listBinaryTree:ListBuffer[BinaryTree]): DataFrame = {

    var S_size = S.count().toInt

    if(S_size <= k){
      return S
    }

    // Melakukan inisialisasi keseluruhan
    var S_temp = S
    var clusters: DataFrame = null
    var r = S.orderBy(rand()).limit(1) //query
    val listColumnName = list_of_all_attribute(spark,S) // list nilai kategorikal r

    // Melakukan inisialisasi while (S_temp.count() >= k)
    var c:DataFrame = null
    var member_cluster:DataFrame = null
    var cluster_name:String = ""
    var numCluster = 1

    // Apabila jumlah data >= k, maka lakukan perulangan
    while (S_size >= k){
      r = S.orderBy(rand()).limit(1)

      // Mencari record tabel S terjauh dengan record r
      r = furthest_record_from_r_optimize(json,S_temp,r,listBinaryTree)

      // Membuang record r dari tabel S (1)
      S_size -= 1
      S_temp = S_temp.except(r).cache()

      // Membuat penamaan sebuah cluster
      cluster_name = "Cluster "+ numCluster
      numCluster += 1

      // Membuat record r terjauh sebagai centroid cluster baru (c)
      c = r
      c.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("hdfs://localhost:50071/skripsi/temp1/")
      var c_size = 1
      val min_max_column = min_max_cluster(spark,c)
      member_cluster = c.withColumn("Cluster",lit(cluster_name))
      member_cluster = member_cluster.withColumn("Info",lit("Centroid"))
      member_cluster.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("hdfs://localhost:50071/skripsi/temp2/")

      // Mencari kelompok data pada cluster terdekat (c)
      while ( c_size < k ) {

        c = spark.read.format("csv").option("header", "true").load("hdfs://localhost:50071/skripsi/temp1/")

        // Mencari record terbaik sebagai anggota cluster (c)
        r = find_best_record(spark,json,S_temp,c,listColumnName,S_size,c_size,listDataType,min_max_column)

        // Mengelompokan data terhadap c -> find best record
        c = c.union(r).cache()
        c.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("hdfs://localhost:50071/skripsi/temp1/")
        c_size += 1

        // Membuang record r dari tabel S (2)
        S_size -= 1
        S_temp = S_temp.except(r)

        // Menambahan kolom nama cluster dan info pada record r
        var r_temp = r.withColumn("Cluster",lit(cluster_name))
        r_temp = r_temp.withColumn("Info",lit("Member"))

        // Mengelompokan data terhadap member_cluster (nama cluster, info) -> output pengelompokan data
        member_cluster = spark.read.format("csv").option("header", "true").load("hdfs://localhost:50071/skripsi/temp2/")
        member_cluster = member_cluster.union(r_temp)
        member_cluster.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("hdfs://localhost:50071/skripsi/temp2/")
      }

      // Mengatasi error union, jika result == null
      member_cluster = member_cluster.crossJoin( min_max_cluster(spark,member_cluster) )
      if(clusters == null) {
        clusters = member_cluster
        clusters.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("hdfs://localhost:50071/skripsi/temp3/")
      }
      else {
        clusters = spark.read.format("csv").option("header", "true").load("hdfs://localhost:50071/skripsi/temp3/")
        clusters = clusters.union(member_cluster)
        clusters.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("hdfs://localhost:50071/skripsi/temp3/")
      }

    }

    // Jika S_temp masih ada sisa (masih ada data yang belum di kelompokan)
    var remainingRecord: DataFrame = null // record with Cluster Name only
    var record: DataFrame = null

    while (S_size > 0){
      // Mengambil record yang tersisa secara acak
      r = S_temp.orderBy(rand()).limit(1).cache()

      // Membuang record r dari tabel S (3)
      S_size -= 1
      S_temp = S_temp.except(r).cache()


      // Mencari cluster terbaik (c) untuk anggota record r
      val min_max_column = clusters.select(clusters.columns.filter(x => x.contains("_") || x.contains("Cluster")).map(clusters(_)) : _*).distinct()
      c = find_best_cluster(spark,json,clusters,r,numCluster,listColumnName,k,listDataType,min_max_column)
      r = r.crossJoin(c)

      // Menampung hasil pengelompokan data
      if(remainingRecord == null) {
        remainingRecord = r.withColumn("Cluster",c("Cluster"))
        clusters.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("hdfs://localhost:50071/skripsi/temp4/")
      }
      else {
        record = r.withColumn("Cluster",c("Cluster"))
        remainingRecord = spark.read.format("csv").option("header", "true").load("hdfs://localhost:50071/skripsi/temp4/")
        remainingRecord = remainingRecord.union(record)
        clusters.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("hdfs://localhost:50071/skripsi/temp4/")
      }

    }

    clusters = clusters.select(clusters.columns.filter(x => !(x.contains("_") || x.contains("Info")) ).map(clusters(_)) : _*)
    if(remainingRecord != null) {
      clusters = spark.read.format("csv").option("header", "true").load("hdfs://localhost:50071/skripsi/temp3/")
      clusters = clusters.union(remainingRecord)
      clusters.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("hdfs://localhost:50071/skripsi/temp3/")
    }
    return clusters

  }

  def list_of_all_attribute(spark: SparkSession, record: DataFrame): List[String] = {
    record.createOrReplaceTempView("tAdults")
    val test = spark.catalog.listColumns("tAdults").select("name","datatype")
    val df = test.select("name")
    val columnName = df.collect().map(_(0)).toList.asInstanceOf[List[String]]
    return columnName
  }

  def find_best_record(spark: SparkSession, json: DataFrame, S: DataFrame, c: DataFrame, listColumnName: List[String], S_size: Int, c_size: Int, listDataType: Array[DataType],min_max_column: DataFrame): DataFrame = {
    val S_temp = S
    val cluster_union_r = S_temp.crossJoin(min_max_column)
    val subs_infoloss = calculate_substraction_information_loss_optimize(json,cluster_union_r,c,c_size)
    val subs_infoloss_ordered = subs_infoloss.orderBy(asc("Subs_IL")).limit(1)
    val best = subs_infoloss_ordered.select(subs_infoloss_ordered.columns.filter(!_.contains("_")).map(subs_infoloss_ordered(_)): _*)
    return best
  }

  def min_max_cluster(spark: SparkSession, c: DataFrame):DataFrame = {
    var result:DataFrame = null
    c.dtypes.filter(_._2 == "IntegerType").foreach{element =>
      if(element._1 != "id" && element._2.contains("Integer")){

        if(result == null){
          result = c.select(max(element._1).as("max_"+element._1),min(element._1).as("min_"+element._1))
        }
        else{
          result = result.withColumn("max_"+element._1,max(element._1))
          result = result.withColumn("min_"+element._1,min(element._1))
        }

      }

    }
    return result
  }

  def find_best_cluster(spark: SparkSession, json: DataFrame, clusters: DataFrame, r: DataFrame, numCluster: Int, listColumnName: List[String], c_size: Int, listDataType: Array[DataType],min_max_column:DataFrame): DataFrame = {
    val cluster_union_r = r.crossJoin(min_max_column)
    val subs_infoloss = calculate_substraction_information_loss_optimize(json,cluster_union_r,clusters,c_size)
    val subs_infoloss_ordered = subs_infoloss.orderBy(asc("Subs_IL")).limit(1)
    val best = subs_infoloss_ordered.select("Cluster")
    return best
  }






}
