from pyspark.sql import SparkSession

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# 读取文本文件
text = spark.sparkContext.textFile("test.txt")

# 执行 WordCount
word_counts = text.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)

# 输出结果
print(word_counts.collect())

# 停止 SparkSession
spark.stop()