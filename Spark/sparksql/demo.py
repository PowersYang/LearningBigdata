import os

from pyspark import SparkConf
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"

if __name__ == '__main__':
    spark_conf = SparkConf().setAppName('SparkSql Demo').setMaster('spark://localhost:7077')

    spark = SparkSession.builder.getOrCreate()

    # df = spark.read.json('../in/user.json')

    # df.show()
    #
    # df.select('name').show()
    #
    # df.select(df['name'], df['age'] + 1).show()

    # df.createGlobalTempView('students')
    #
    # spark.sql('select name, age from global_temp.students').show()

    rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
    rdd.foreach(lambda x: print(x))

    spark.stop()
