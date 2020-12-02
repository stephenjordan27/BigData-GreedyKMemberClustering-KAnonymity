package ExploratoryModel

import ExaminationModel.MainExamination.{get_attribute_datatype_json, get_attribute_name_json, read_element_from_json}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object MainExploratory {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local[*]")
      .appName("SparkSQL")
      .getOrCreate()

    // Parameter Pengujian
    val path_JSON = args(0)
    val json: DataFrame = spark.read.option("multiline", "true").json(path_JSON).cache() // ganti di sini
    val path_data_input = json.select("input_path").first().getString(0)                           // ganti di sini
    val datasetInput = spark.read.format("csv").option("header", "true").load(path_data_input)
    val columnSelectionWithID = generate_dataframe_from_csv(spark,json,datasetInput)

    import org.apache.spark.sql.functions._
    val outputPathExploratoryAnalysis = json.select("output_path").first().getString(0)
    val columnsModify = columnSelectionWithID.columns.map(col).map(colName => {
    val attrDF = columnSelectionWithID.select(colName).distinct()
    attrDF.coalesce(1)
            .write
            .option("header","true")
            .option("sep",",")
            .mode("overwrite")
            .csv(outputPathExploratoryAnalysis+colName)
    })


  }

  def generate_dataframe_from_csv(spark:SparkSession, json:DataFrame, dataInput: DataFrame):DataFrame={
    val selectedColumn:ListBuffer[Seq[String]] = read_element_from_json(json,"selected_column")

    val selectedColumnAttribute:List[String] = get_attribute_name_json(selectedColumn).toList
    val selectedColumnDatatype:List[String] = get_attribute_datatype_json(selectedColumn).toList

    var columnSelectionDF = dataInput.select(selectedColumnAttribute.head,selectedColumnAttribute.tail: _*).toDF() // ganti di sini

    for(i <- 0 until selectedColumnAttribute.length){

      val attrName = selectedColumnAttribute(i)
      val datatype = selectedColumnDatatype(i)


      if(datatype == "category"){
        columnSelectionDF = columnSelectionDF.withColumn(attrName+"_new",columnSelectionDF(attrName).cast(StringType))
        columnSelectionDF = columnSelectionDF.drop(attrName)
      }
      else{
        columnSelectionDF = columnSelectionDF.withColumn(attrName+"_new",columnSelectionDF(attrName).cast(IntegerType))
        columnSelectionDF = columnSelectionDF.drop(attrName)
      }

    }

    val columnSelectionRenamedDF = columnSelectionDF.toDF(selectedColumnAttribute: _*)

    return columnSelectionRenamedDF
  }

}
