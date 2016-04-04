from __future__ import print_function
from pyspark import SparkContext, RDD

logFile = "$YOUR_SPARK_HOME/README.md"  # Should be some file on your system
sc = SparkContext("local", "main")
logData = sc.textFile(logFile).cache()

movies_file = sc.textFile("data/movie_titles.txt")
movies_rdd = movies_file.map(lambda line: line.split(","))
print(movies_rdd.count())				# Movies count
print(movies_rdd.foreach(print))
