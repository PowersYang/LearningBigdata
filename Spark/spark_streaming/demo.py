import os

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3'

sc = SparkContext('local[*]', 'WordCount')
ssc = StreamingContext(sc, 1)

lines = ssc.socketTextStream('localhost', 9999)

# 更换数据源
# lines = ssc.textFileStream("../in/streaming")

words = lines.flatMap(lambda line: line.split(' '))

pairs = words.map(lambda word: (word, 3))

word_counts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
word_counts.pprint()

ssc.start()
ssc.awaitTermination()