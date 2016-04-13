from __future__ import print_function
from pyspark import SparkContext, RDD

logFile = "$YOUR_SPARK_HOME/README.md"  # Should be some file on your system
sc = SparkContext("local", "main")
logData = sc.textFile(logFile).cache()

movies_file = sc.textFile("data/movie_titles.txt")
movies_rdd = movies_file.map(lambda line: line.split(","))
#print(movies_rdd.count())				# Movies count
#print(movies_rdd.foreach(print)) 		# Print Movies from Movies File

ratings_file = sc.textFile("/Users/Rocha/Documents/Datasets/download/train100/mv_00[0-9]*.txt")

results_file = sc.textFile("data/probe.txt")

test = ""


def change_name(x):
	global test
	if ':' in x:
		test = x[:-1]+","
		return x
	else:
		return test+x

#information from training_set
ratings_with_id = ratings_file.map(lambda x: change_name(x))
ratings_filtered = ratings_with_id.filter(lambda x: ':' not in x)
ratings_rdd = ratings_filtered.map(lambda line: line.split(','))

#print(ratings_rdd.take(26))

#information from probe file
results_with_id = results_file.map(lambda x: change_name(x))
results_filtered = results_with_id.filter(lambda x: ':' not in x)
results_rdd = results_filtered.map(lambda line: line.split(','))

#print(results_rdd.take(26))

#join training_set information with probe file information to have the ratings

rat_rdd = ratings_rdd.map(lambda x: ((x[0],x[1]),[x[2],x[3]]))
#print(rat_rdd.first())

res_rdd = results_rdd.map(lambda x: ((x[0],x[1]),[]))
#print(res_rdd.first())
#print(res_rdd.count())

join_rdd = rat_rdd.join(res_rdd)
print(join_rdd.count())
#print(join_rdd.first())

