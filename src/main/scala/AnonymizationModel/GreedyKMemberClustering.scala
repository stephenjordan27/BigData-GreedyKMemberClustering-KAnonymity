package AnonymizationModel

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{max, _}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.mutable
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

  def calculateNumericalInformationLossUnion(cluster_size:Int) = udf ( (max_num: Int,min_num: Int, num:Int) => {
    var max_value = max_num
    var min_value = min_num
    if(num > max_num){
      max_value = num
    }
    if(num < min_num){
      min_value = num
    }
    Math.abs(max_value-min_value)*1.0/cluster_size
  })

  def calculateCategoricalDistance(binaryTree: BinaryTree):UserDefinedFunction = udf( (category1: String, category2: String) => {
    if(binaryTree!=null){
      val node1 = binaryTree.search(category1)
      val node2 = binaryTree.search(category2)

      if(node1.name != node2.name){
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

  def calculateCategoricalInformationLoss(binaryTree: BinaryTree,listDistinctValues:ListBuffer[String]):UserDefinedFunction = udf( () => {
    if(binaryTree!=null){

      val numDistinctValues = listDistinctValues.length

      if(numDistinctValues == 2){
        val node1 = binaryTree.search(listDistinctValues(0))
        val node2 = binaryTree.search(listDistinctValues(1))
        val LCA = new LowestCommonAncestor()
        val LCA_root_name = LCA.findLCA(binaryTree.root, node1.name, node2.name)
        val H_subtree = binaryTree.search(LCA_root_name).level
        val H_TD = binaryTree.getHeight(binaryTree.root).toDouble
        BigDecimal(H_subtree*1.0 / H_TD).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
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

  def calculateCategoricalInformationLossUnion(binaryTree: BinaryTree,listDistinctValues:Seq[Any]):UserDefinedFunction = udf( (category: String) => {
    if(binaryTree!=null){

      var listDistinctValuesUpdate = listDistinctValues
      val check = listDistinctValues.contains(category)
      if(!check) listDistinctValuesUpdate += category
      val numDistinctValues = listDistinctValuesUpdate.length

      if(numDistinctValues == 2){
        val node1 = binaryTree.search(listDistinctValues(0).toString)
        val node2 = binaryTree.search(listDistinctValues(1).toString)
        val LCA = new LowestCommonAncestor()
        val LCA_root_name = LCA.findLCA(binaryTree.root, node1.name, node2.name)
        val H_subtree = binaryTree.search(LCA_root_name).level
        val H_TD = binaryTree.getHeight(binaryTree.root).toDouble
        BigDecimal(H_subtree*1.0 / H_TD).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
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

  def furthest_record_from_r_optimize(json: DataFrame, S: DataFrame, r:DataFrame): DataFrame = {
    val listColumnName = S.columns.toSeq
    var S_temp = S.withColumn("code", lit(1))
    var r_temp = r.withColumn("code", lit(1))

    r_temp = r_temp.select(r_temp.columns.map(c => col(c).alias(c+"-r")): _*)
    S_temp = S_temp.join(r_temp,r_temp("code-r") ===  S_temp("code"),"cross")
    S_temp =  S_temp.drop("code","code-r")


    S_temp.dtypes.foreach(element =>
      if(!element._1.contains("-r") && !element._1.contains("id")){
        if(element._2.contains("Integer")){
          val max_value = S_temp.select(max(element._1)).first()(0)
          val min_value = S_temp.select(min(element._1)).first()(0)
          S_temp = S_temp.withColumn("max_"+element._1,lit(max_value))
          S_temp = S_temp.withColumn("min_"+element._1,lit(min_value))
          S_temp = S_temp.withColumn("dist_"+element._1,calculateNumericalDistance(col(element._1),col(element._1+"-r"),col("max_"+element._1),col("min_"+element._1) ) )
        }
        else{
          val dgh = read_dgh_from_json(json, element._1)
          var binaryTree:BinaryTree = null
          if(dgh!=null){
            val treeName = dgh.remove(0)(0)
            binaryTree = create_binary_tree_from_dgh_attribute(dgh)
          }
          else{
            binaryTree = null
          }
          S_temp = S_temp.withColumn("dist_"+element._1,calculateCategoricalDistance(binaryTree)( S_temp(element._1),S_temp(element._1+"-r") ))
        }
      }
    )

    val distance = S_temp.select(S_temp.columns.filter(_.startsWith("dist_")).map(S_temp(_)) : _*)
    val columnstosum = distance.columns.toSeq.map(col _)

    S_temp = S_temp.withColumn("dist_record",sum_(columnstosum: _*))
    S_temp = S_temp.orderBy(desc("dist_record"))

    val furthest_record_from_r = S_temp.select(listColumnName.head, listColumnName.tail: _*).limit(1)

    return furthest_record_from_r
  }

  def calculate_information_loss_optimize(json: DataFrame, cluster: DataFrame,r: DataFrame,cluster_size:Int,listMaxValueUpdated:ListBuffer[Int],listMinValueUpdated:ListBuffer[Int],listDistinctColumnValues:ListBuffer[ListBuffer[String]]): DataFrame = {
    var cluster_temp = cluster.withColumn("code", lit(1))

    var r_temp = r.withColumn("code", lit(1))
    r_temp = r_temp.select(r_temp.columns.map(c => col(c).alias(c+"-r")): _*)

    cluster_temp = cluster_temp.join(r_temp,r_temp("code-r") ===  cluster_temp("code"),"cross")
    cluster_temp =  cluster_temp.drop("code","code-r")

    var num_index = 0
    var cat_index = 0
    cluster.dtypes.foreach(element =>
      if(!element._1.contains("-r") && !element._1.contains("id")){
        if(element._2.contains("Integer")){
          val max_value = listMaxValueUpdated(num_index)
          val min_value = listMinValueUpdated(num_index)
          cluster_temp = cluster_temp.withColumn("max_"+element._1,lit(max_value))
          cluster_temp = cluster_temp.withColumn("min_"+element._1,lit(min_value))
          cluster_temp = cluster_temp.withColumn("il_c_"+element._1,calculateNumericalInformationLoss(cluster_size)(col("max_"+element._1),col("min_"+element._1) ))
          cluster_temp = cluster_temp.withColumn("il_u_"+element._1,calculateNumericalInformationLossUnion(cluster_size)(col("max_"+element._1),col("min_"+element._1),col(element._1+"-r")))
          num_index += 1
        }
        else{
//          val listDistinctValues = cluster.select(element._1).distinct().collect()
          val listDistinctValues = listDistinctColumnValues(cat_index)
          val dgh = read_dgh_from_json(json, element._1)
          var binaryTree:BinaryTree = null
          if(dgh!=null){
            val treeName = dgh.remove(0)(0)
            binaryTree = create_binary_tree_from_dgh_attribute(dgh)
          }
          else{
            binaryTree = null
          }
          cluster_temp = cluster_temp.withColumn("il_c_"+element._1,calculateCategoricalInformationLoss(binaryTree,listDistinctValues)( ))
          cluster_temp = cluster_temp.withColumn("il_u_"+element._1,calculateCategoricalInformationLossUnion(binaryTree,listDistinctValues)(cluster_temp(element._1+"-r") ))
          cat_index += 1
        }
      }
    )

    val infoloss_cluster = cluster_temp.select(cluster_temp.columns.filter(_.startsWith("il_c")).map(cluster_temp(_)) : _*)
    val sum_infoloss_cluster = infoloss_cluster.columns.toSeq.map(col _)

    val infoloss_union = cluster_temp.select(cluster_temp.columns.filter(_.startsWith("il_u")).map(cluster_temp(_)) : _*)
    val sum_infoloss_union = infoloss_union.columns.toSeq.map(col _)

    cluster_temp = cluster_temp.withColumn("total_il_c",sum_(sum_infoloss_cluster: _*)*cluster_size)
    cluster_temp = cluster_temp.withColumn("total_il_u",sum_(sum_infoloss_union: _*)*cluster_size)
    cluster_temp = cluster_temp.withColumn("subs_il-r",col("total_il_u")-col("total_il_c"))

    cluster_temp = cluster_temp.select(cluster_temp.columns.filter(_.contains("-r")).map(cluster_temp(_)) : _*).limit(1)
    return cluster_temp
    //    cluster_temp = cluster_temp.select("subs_il-r").limit(1)
    //    return cluster_temp.collect()(0)(0).asInstanceOf[Double]
  }

  
  def greedy_k_member_clustering(spark: SparkSession, sc: SparkContext, json: DataFrame, S: DataFrame, k: Int, listDataType: Array[DataType]): DataFrame = {

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
//      r = furthest_record_from_r(spark,S_temp,r,json) // 5 detik
      r = furthest_record_from_r_optimize(json,S_temp,r)

      // Membuang record r dari tabel S (1)
      S_size -= 1
      S_temp = S_temp.except(r).repartition(getNumPartitions(S_size)).cache()

      // Membuat penamaan sebuah cluster
      cluster_name = "Cluster "+ numCluster
      numCluster += 1

      // Membuat record r terjauh sebagai centroid cluster baru (c)
      c = r
      var c_size = 1
      member_cluster = c.withColumn("Cluster",lit(cluster_name))
      member_cluster = member_cluster.withColumn("Info",lit("Centroid"))

      // Mencari max dan min atribut numerik (1)
      val columns_c = c.drop("id").schema.fields.filter(x => x.dataType == IntegerType)
      val listMaxValue = c.drop("id").select(columns_c.map(x => col(x.name)): _*).first()
      val listMinValue = listMaxValue

      // Mencari nilai unik atribut kategorikal (1)
      val cat_columns_c = c.schema.fields.filter(x => x.dataType == StringType)
      val listDistinctValue = c.select(cat_columns_c.map(x => col(x.name)): _*).first()
      val listDistinctColumnValues = new ListBuffer[ListBuffer[String]]

      for(i <- 0 until listDistinctValue.length){
        val distinctValues = new ListBuffer[String]
        distinctValues += listDistinctValue(i).toString
        listDistinctColumnValues += distinctValues
      }

      // Mencari kelompok data pada cluster terdekat (c)
      while ( c_size < k ) {

        // Mencari max dan min atribut numerik (2)
        val listMaxValueUpdated = new ListBuffer[Int]
        val listMinValueUpdated = new ListBuffer[Int]
        val columns_r = r.drop("id").drop("id-r").schema.fields.filter(x => x.dataType == IntegerType)
        val listIntegerValue = r.drop("id").drop("id-r").select(columns_r.map(x => col(x.name)): _*).first()

        for(i <- 0 until listMaxValue.length){
          // Max value
          if(listIntegerValue(i).asInstanceOf[Int]>listMaxValue(i).asInstanceOf[Int]){
            listMaxValueUpdated += listIntegerValue(i).asInstanceOf[Int]
            // Min value
            if(listIntegerValue(i).asInstanceOf[Int]<listMinValue(i).asInstanceOf[Int]){
              listMinValueUpdated += listIntegerValue(i).asInstanceOf[Int]
            }
            else{
              listMinValueUpdated += listMaxValue(i).asInstanceOf[Int]
            }
          }
          else {
            listMaxValueUpdated += listMaxValue(i).asInstanceOf[Int]
            // Min value
            if(listIntegerValue(i).asInstanceOf[Int]<listMinValue(i).asInstanceOf[Int]){
              listMinValueUpdated += listIntegerValue(i).asInstanceOf[Int]
            }
            else{
              listMinValueUpdated += listMaxValue(i).asInstanceOf[Int]
            }
          }

        }

        // Mencari nilai unik atribut kategorikal (2)
        val cat_columns_r = r.schema.fields.filter(x => x.dataType == StringType)
        val listDistinctValueUpdated = r.select(cat_columns_r.map(x => col(x.name)): _*).first()
        val listDistinctColumnValuesUpdated = new ListBuffer[ListBuffer[String]]

        for(i <- 0 until listDistinctValue.length){
          if( !listDistinctColumnValues(i).contains(listDistinctValueUpdated(i)) ){
            val distinctValues = listDistinctColumnValues(i)
            distinctValues += listDistinctValueUpdated(i).toString
            listDistinctColumnValuesUpdated += distinctValues
          }
          else{
            listDistinctColumnValuesUpdated += listDistinctColumnValues(i)
          }
        }



        // Mencari record terbaik sebagai anggota cluster (c)
        r = find_best_record(spark,sc,json,S_temp,c,listColumnName,S_size,c_size,listDataType,listMaxValueUpdated,listMinValueUpdated,listDistinctColumnValuesUpdated)

        // Mengelompokan data terhadap c -> find best record
        c = c.union(r).repartition(1).cache()
        c_size += 1


        // Membuang record r dari tabel S (2)
        S_size -= 1
        S_temp = S_temp.except(r).repartition(getNumPartitions(S_size)).cache()


        // Menambahan kolom nama cluster dan info pada record r
        var r_temp = r.withColumn("Cluster",lit(cluster_name))
        r_temp = r_temp.withColumn("Info",lit("Member"))

        // Mengelompokan data terhadap member_cluster (nama cluster, info) -> output pengelompokan data
        member_cluster = member_cluster.union(r_temp).repartition(1).cache()
      }

      // Mengatasi error union, jika result == null
      if(clusters == null) clusters = member_cluster.cache()
      else clusters = clusters.union(member_cluster).repartition(1).cache()

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
      S_size -= 1
      S_temp = S_temp.except(r).repartition(getNumPartitions(S_size)).cache()


      // Mencari cluster terbaik (c) untuk anggota record r
      c = find_best_cluster(spark,json,clusters,r,listClusterName,listColumnName,k,listDataType)
      clusterName = c.select("Cluster").first().getString(0)

      // Menampung hasil pengelompokan data
      if(remainingRecord == null) {
        remainingRecord = r.withColumn("Cluster",lit(clusterName))
      }
      else {
        record = r.withColumn("Cluster",lit(clusterName))
        remainingRecord = remainingRecord.union(record).distinct().repartition(1).cache()
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

  def find_best_record(spark: SparkSession, sc: SparkContext, json: DataFrame, S: DataFrame, c: DataFrame, listColumnName: List[String], S_size: Int, c_size: Int, listDataType: Array[DataType],listMaxValueUpdated:ListBuffer[Int],listMinValueUpdated:ListBuffer[Int],listDistinctColumnValues:ListBuffer[ListBuffer[String]]): DataFrame = {
    var best:DataFrame = null
//    var min:Double = Int.MaxValue
    var unclustered_record_size = S_size
    var unclustered_record = S

    while(unclustered_record_size > 0){
      val r = unclustered_record.limit(1).cache()//10 detik
      if(best == null){
        best = calculate_information_loss_optimize(json,c,r,c_size,listMaxValueUpdated,listMinValueUpdated,listDistinctColumnValues)
      }
      else{
        best = best.union(calculate_information_loss_optimize(json,c,r,c_size,listMaxValueUpdated,listMinValueUpdated,listDistinctColumnValues))
      }

//      val diff = calculate_information_loss_optimize(json,c,r,c_size,listMaxValueUpdated,listMinValueUpdated)
//      if(diff < min){
//        min = diff
//        best = r
//      }
      unclustered_record_size -= 1
      unclustered_record = unclustered_record.except(r).repartition(getNumPartitions(unclustered_record_size))

      //  val diff = calculate_substraction_information_loss(spark,json,c,r,listColumnName,c_size, listDataType,information_loss_c) // 38 detik - 1 iterasi                 //error
    }

    best = best.orderBy(asc("subs_il-r"))
    best = best.limit(1).drop("subs_il-r")

    //    var information_loss_c = 0.0
    //    if(c_size > 1){
    //      val num_distinct_values_c = c.select(c.columns.map(c => countDistinct(col(c)).alias(c) ): _*).first()
    //      information_loss_c = calculate_information_loss(spark,json,c,c_size,listColumnName,listDataType,num_distinct_values_c)
    //    }


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
      val num_distinct_values_c = c.select(c.columns.map(c => countDistinct(col(c)).alias(c) ): _*).first()
      val information_loss_c = calculate_information_loss(spark,json,c,c_size,listColumnName,listDataType,num_distinct_values_c)
      val c_temp = c.drop("Cluster","Info").cache

      // Menghitung selisih information loss
      val diff = calculate_substraction_information_loss(spark,json,c_temp,r,listColumnName,c_size, listDataType,information_loss_c)
      if (diff < min) {
        min = diff
        best = c
      }

    }
    return best
  }


  // https://stackoverflow.com/questions/39355149/how-to-read-json-with-schema-in-spark-dataframes-spark-sql
  def read_dgh_from_json(json: DataFrame,category: String): ListBuffer[Seq[String]] = {
    var result = ListBuffer[Seq[String]]()

    try{
      val tree = json.select("domain_generalization_hierarchy."+ category+".tree").collect()(0).getString(0)
      val dgh_json = json.select("domain_generalization_hierarchy." + category+".generalization")
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

    val S_temp = S
    val S_temp_size = S_temp.count()

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
              val subs_maxmin_value = S.select(max(colName)-min(colName)).first().getInt(0)// query
              record_distance += calculate_numeric_distance(num1, num2, subs_maxmin_value)
            }
            // Menghitung distance kategorikal

            else {
              val dgh = read_dgh_from_json(json, "age")
              if(dgh != null){
                val treeName = dgh.remove(0)(0)
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

  def calculate_numeric_distance(num1: Int, num2: Int, subs_maxmin_value:Int): Double = {
    try{
      val result:Double =  Math.abs(num1-num2)*1.0/Math.abs(subs_maxmin_value)
      return BigDecimal(result).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    }
    catch {
      case x: Exception => {
        return 0.0
      }
    }
  }

  def calculate_numeric_distance_information_loss(subs_maxmin_value: Int,cluster_size: Int): Double = {
    var result:Double = 0.0
    try{
      result = Math.abs(subs_maxmin_value)*1.0/cluster_size
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
      result = H_subtree*1.0 / H_TD
    }
    result = BigDecimal(result).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    return result
  }

  def calculate_categorical_distance_information_loss(dgh:ListBuffer[Seq[String]]): Double = {
    var result:Double = 0.0
    val binaryTree = create_binary_tree_from_dgh_attribute(dgh)
    val H_subtree = 1.0
    val H_TD = binaryTree.getHeight(binaryTree.root)
    result = H_subtree*1.0 / H_TD
    result = BigDecimal(result).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    return result
  }

  def calculate_record_distance(numeric_distance:Double,categorical_distance:Double): Double ={
    return numeric_distance+categorical_distance
  }

  def calculate_substraction_information_loss(spark: SparkSession, json: DataFrame, c: DataFrame, r : DataFrame, listColumnName: List[String], c_size:Int, listDataType: Array[DataType],information_loss_c:Double): Double={
    val union = c.union(r).repartition(1).cache()
    val num_distinct_values_union = union.select(union.columns.map(c => countDistinct(col(c)).alias(c) ): _*).first()
    val information_loss_c_union_r = calculate_information_loss(spark,json,union,c_size+1,listColumnName,listDataType,num_distinct_values_union)
    val substraction_information_loss = information_loss_c_union_r - information_loss_c
    return substraction_information_loss
  }

  def calculate_information_loss(spark: SparkSession, json: DataFrame,  cluster: DataFrame, clusterSize: Int, listColumnName: List[String], listDataType: Array[DataType],num_distinct_values:Row): Double = {
    import org.apache.spark.sql.functions._

    var information_loss:Double = 0.0

    listColumnName.zipWithIndex.foreach { case(colName,i) =>  // looping
      if(colName != "id") {
        // Menghitung distance numerik information loss
        if(listDataType(i).isInstanceOf[IntegerType]){
          val subs_maxmin_value = cluster.select(max(colName)-min(colName)).first().getInt(0)// query
          information_loss += calculate_numeric_distance_information_loss(subs_maxmin_value,clusterSize)
        }
        // Menghitung distance kategorikal
        else {
          val dgh = read_dgh_from_json(json, colName)
          if(dgh != null) {
            val treeName = dgh.remove(0)(0)
            if(num_distinct_values(i).toString.toInt > 2) {
              information_loss += calculate_categorical_distance_information_loss(dgh)
            }
            else if(num_distinct_values(i).toString.toInt == 2) {
              val distinctValue = cluster.select(colName).distinct().collect()
              val category1 =  distinctValue(0)(0).toString
              val category2 = distinctValue(1)(0).toString
              information_loss += calculate_categorical_distance(dgh,category1,category2)
            }
            else{
              information_loss += 0
            }
          }
        }
      }
    }
    return clusterSize*information_loss
  }

}
