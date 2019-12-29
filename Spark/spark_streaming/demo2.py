import os

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3'


def updateFunc(new_values, last_sum):
    if last_sum is None:
        last_sum = 0
    return sum(new_values) + (last_sum or 0)


if __name__ == '__main__':
    sc = SparkContext('local[*]', 'WordCount')
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint('checkpoint')

    lines = ssc.socketTextStream('localhost', 9999)

    words = lines.flatMap(lambda line: line.split(' '))

    pairs = words.map(lambda word: (word, 1))

    word_counts = pairs.updateStateByKey(updateFunc)

    word_counts.pprint()

    ssc.start()
    ssc.awaitTermination()
