from __future__ import print_function
from pyspark import SparkContext, RDD
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel
from math import sqrt

logFile = "$YOUR_SPARK_HOME/README.md"  # Should be some file on your system
sc = SparkContext("local", "main")

#ratings_file = sc.textFile("/Users/Rocha/Documents/Datasets/download/training_set/mv_00[0-9]*") <- full training set
#ratings_rdd_strings = ratings_file.map(lambda line: line.split(','))

ratings_file = sc.textFile("/Users/Rocha/Documents/Datasets/download/train1500/mv_00[0-9]*") #training subset with 1500 movies instead of 1770
ratings_rdd_strings = ratings_file.map(lambda line: line.split(','))
print("First ratings_rdd_strings: "+str(ratings_rdd_strings.first()))

def parse_ratings(x):      
	user_id = int(x[0])      
	movie_id = int(x[1])      
	rating = float(x[2])           
	return [user_id,movie_id,rating,x[3]] 

def parse_probe(x):      
	user_id = int(x[0])      
	movie_id = int(x[1])      
	rating = float(x[2])           
	return [user_id,movie_id,rating] 


ratings_rdd = ratings_rdd_strings.map(lambda x: parse_ratings(x))
#print("First ratings_rdd: "+str(ratings_rdd.first()))
train_data = ratings_rdd.map(lambda x: (x[0], x[1], x[2]))
#print("First train_data: "+str(train_data.first()))

probe_file = sc.textFile("data/probe.txt")
probe_rdd_strings = probe_file.map(lambda line: line.split(','))
#print("First probe_rdd_strings: "+str(probe_rdd_strings.first()))
probe_rdd_total = probe_rdd_strings.map(lambda x: parse_probe(x))
#print("First probe_rdd: "+str(probe_rdd_total.first()))
limite = 1500
probe_rdd = probe_rdd_total.filter(lambda x: x[0] <= limite)


testdata = probe_rdd.map(lambda p: (p[0], p[1]))
print("First testdata (dados a prever): "+str(testdata.first()))

#LOAD MODEL IF ALREADY TRAINED AND SAVED
model = MatrixFactorizationModel.load(sc, "models/rank9iterations3")

#rank = 9
#numIterations = 3    
#print("A executar o ALS sobre o train_data com rank igual a "+str(rank)+" e numero de iteracoes "+str(numIterations))
#model = ALS.train(train_data, rank, numIterations)

predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
#model.save(sc, "models/rank9iterations3")

probe_adaptado = probe_rdd.map(lambda r: ((r[0], r[1]), r[2]))
#print("First previsoes: "+str(predictions.first()))
predicions_vs_ratings = probe_adaptado.join(predictions)
#print("First previsoes vs rating: "+str(predicions_vs_ratings.first()))
#print(predicions_vs_ratings.take(25))

RMSE = sqrt(predicions_vs_ratings.map(lambda r:  (r[1][0] - r[1][1])**2).reduce(lambda x, y: x + y)/predicions_vs_ratings.count())
print("RMSE: "+ str(RMSE))