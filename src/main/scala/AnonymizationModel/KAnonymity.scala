package AnonymizationModel

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

class KAnonymity {

  def k_anonymity(spark: SparkSession,json:DataFrame,clusters: DataFrame, listBinaryTree:ListBuffer[BinaryTree],hdfs:FileSystem,path_HDFS:String,path_delete_function_HDFS:String,sensitive_identifier:String): DataFrame ={
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
      clusterDF.coalesce(1).write.format("parquet").save(path_HDFS+"/kanonymity2_tmp/")

      // Melakukan iterasi untuk anonimisasi data
      var i = 1
      clusterDF.dtypes.foreach { element =>  // looping

        if(element._1 != "id" && !element._1.contains("Cluster") && !element._1.contains("min_") && !element._1.contains("max_") && element._1 != sensitive_identifier) {

          if (element._2.contains("Integer")) {
            // Membaca file HDFS
            this.delete_folder_hdfs(path_delete_function_HDFS+"/kanonymity2/",hdfs)
            hdfs.rename(new Path(path_HDFS+"/kanonymity2_tmp"),
                        new Path(path_HDFS+"/kanonymity2"))
            clusterDF = spark.read.parquet(path_HDFS+"/kanonymity2/")

            // Menulis ke file HDFS
            clusterDF = clusterDF.withColumn("Anonym_"+element._1,convert_to_numeric_anonymous(
                                  col("max_"+element._1),col("min_"+element._1)))
            clusterDF.coalesce(1).write.format("parquet").save(path_HDFS+"/kanonymity2_tmp/")

            i += 1
          }
          else {
            val binaryTree = listBinaryTree(i)

            // Membaca file HDFS
            this.delete_folder_hdfs(path_delete_function_HDFS+"/kanonymity2/",hdfs)
            hdfs.rename(new Path(path_HDFS+"/kanonymity2_tmp"),
                        new Path(path_HDFS+"/kanonymity2"))
            clusterDF = spark.read.parquet(path_HDFS+"/kanonymity2/")

            // Menulis file ke HDFS
            val distinctColumn = clusterDF.select(collect_set(element._1).as("Dist_"+element._1))

            if (binaryTree != null) {
              clusterDF = clusterDF.crossJoin(distinctColumn)
              clusterDF = clusterDF.withColumn("Anonym_"+element._1,
                          convert_to_category_anonymous(binaryTree)(col("Dist_"+element._1)) )
              clusterDF = clusterDF.drop("Dist_"+element._1)
              clusterDF.coalesce(1).write.format("parquet").save(path_HDFS+"/kanonymity2_tmp/")

              i += 1
            }
            else {
              clusterDF = clusterDF.crossJoin(distinctColumn)
              clusterDF = clusterDF.withColumn("Anonym_"+element._1,
                          col("Dist_"+element._1).getItem(0) )
              clusterDF = clusterDF.drop("Dist_"+element._1)
              clusterDF.coalesce(1).write.format("parquet").save(path_HDFS+"/kanonymity2_tmp/")
              i += 1
            } // end else

          } // end else

        } // end if

      } // end loop

      // Membaca file HDFS
      this.delete_folder_hdfs(path_delete_function_HDFS+"/kanonymity2/",hdfs)
      hdfs.rename(new Path(path_HDFS+"/kanonymity2_tmp"),
                  new Path(path_HDFS+"/kanonymity2"))
      clusterDF = spark.read.parquet(path_HDFS+"/kanonymity2/")
      val clusterDF_temp2 = clusterDF.select("id",sensitive_identifier,"Cluster").
                            withColumnRenamed("id","id_temp")


      // Mengambil nama column diawali dengan "Anonym"
      clusterDF = clusterDF.select(clusterDF.columns.
                  filter(colName => colName.startsWith("Anonym")||
                  colName.contains("id")).
                  map(clusterDF(_)) : _*)

      clusterDF = clusterDF.join(clusterDF_temp2,clusterDF("id")===clusterDF_temp2("id_temp"),"inner")
      clusterDF = clusterDF.drop("id_temp")

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

    result = spark.read.format("csv").option("header", "true").schema(result.schema).
                        load(path_HDFS+"/kanonymity3/")
    result = result.orderBy(asc("id"))
    result = result.drop("Cluster")

    return result
  }

  def convert_to_numeric_anonymous = udf ( (maxValue: Int, minValue: Int) => {
    if(maxValue == minValue){
      maxValue.toString
    }
    else{
      var maxValueTemp = maxValue.toString
      var minValueTemp = minValue.toString
      if(maxValue < 0){
        maxValueTemp = "("+maxValueTemp+")"
      }
      if(minValue < 0){
        minValueTemp = "("+minValueTemp+")"
      }
      "[" + minValueTemp + "-" + maxValueTemp + "]"
    }
  })

  def convert_to_category_anonymous(binaryTree: BinaryTree) = udf ( (listCategory: Seq[String]) => {
    if(listCategory.length > 1){
      binaryTree.root.name
    }
    else if (listCategory.length == 1){
      listCategory.head.toString()
    }
    else{
      null
    }

  })

  def delete_folder_hdfs(pathName: String,hdfs:FileSystem) {
    val path = new Path(pathName)
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)
    }
  }


  def getNumPartitions(size:Int):Int = {
    var numPartitions = 1
    if(size >= 9)  numPartitions = size/9
    return numPartitions
  }

}
