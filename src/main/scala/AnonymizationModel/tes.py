import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]") \
        .appName('GMKC') \
        .getOrCreate()

# spark.read.format("csv").option("header", "true").load("C:/Users/asus/IdeaProjects/BigData-GreedyKMemberClustering-KAnonymity/input/adult100k.csv").show()