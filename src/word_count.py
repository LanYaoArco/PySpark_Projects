import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)


def normalize_word(text):
    text = re.sub(r'[^a-z]', ' ', text.lower())
    return text.split()


input = sc.textFile("../data/book")
words = input.flatMap(lambda x: normalize_word(x))

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
sorted_word_count = wordCounts.map(lambda (x, y): (y, x)).sortByKey()
result = sorted_word_count.collect()

for count, word in result:
    if word:
        print(str(count) + " " + word)
