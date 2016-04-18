from __future__ import print_function
from pyspark import SparkContext, RDD
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel
from math import sqrt
from os.path import isdir

def parseRatings(x):      
	user_id = int(x[0])      
	movie_id = int(x[1])      
	rating = float(x[2])           
	return [user_id,movie_id,rating,x[3]] 

def parseProbe(x):      
	user_id = int(x[0])      
	movie_id = int(x[1])      
	rating = float(x[2])           
	return [user_id,movie_id,rating] 

def loadFiles(ratings_dir, limit):
	ratings_file = sc.textFile(ratings_dir+"/mv_00[0-9]*")
	ratings_rdd = ratings_file.map(lambda line: line.split(','))
	ratings_rdd = ratings_rdd.map(lambda x: parseRatings(x))
	ratings_rdd = ratings_rdd.map(lambda x: (x[0], x[1], x[2]))
	probe_file = sc.textFile("data/probe.txt")
	probe_rdd = probe_file.map(lambda line: line.split(','))
	probe_rdd = probe_rdd.map(lambda x: parseProbe(x))
	probe_rdd = probe_rdd.filter(lambda x: x[0] <= limit) #probe_rdd with correct ratings
	test_data = probe_rdd.map(lambda p: (p[0], p[1])) #using probe_rdd without the ratings, so we can predict them
	return (ratings_rdd,probe_rdd,test_data)

def trainModel(data,rank,num_iterations):
	save_file = "models/rank"+str(rank)+"iterations"+str(num_iterations)
	if isdir(save_file):
		print("Model already exists, loading...")
		model = MatrixFactorizationModel.load(sc, save_file)
	else:
		print("Model does not exist, training ALS with rank "+str(rank)+" and "+str(num_iterations)+" iterations")
		model = ALS.train(data, rank, num_iterations)
		print("Saving new model")
		model.save(sc,"save_file")
	return model

def computeRMSE(model, test_data, real_data):
	predictions = model.predictAll(test_data).map(lambda r: ((r[0], r[1]), r[2]))
	probe_adaptado = real_data.map(lambda r: ((r[0], r[1]), r[2]))
	predictions_vs_ratings = probe_adaptado.join(predictions)
	RMSE = sqrt(predictions_vs_ratings.map(lambda r:  (r[1][0] - r[1][1])**2).reduce(lambda x, y: x + y)/predictions_vs_ratings.count())
	return RMSE

def findBestModel(train_data, test_data, real_data):
	ranks = [4,8,9,10,12]
	num_iterations = 10
	best_RMSE = float("inf")
	best_rank = -1
	best_num_iterations = -1
	for rank in ranks:
		model = trainModel(train_data,rank,num_iterations)
		RMSE = computeRMSE(model, test_data, real_data)
		if RMSE < best_RMSE:
			best_RMSE = RMSE
			best_rank = rank
			best_num_iterations = num_iterations
			best_model = model
	print("Best model is with rank "+str(best_rank)+" and "+str(num_iterations)+" iterations with a RMSE = "+str(best_RMSE))
	return best_model


if __name__ == "__main__":
	sc = SparkContext("local", "main") #Standalone version
	ratings_dir = "/Users/Rocha/Documents/Datasets/download/train1500"
	limit = 1500 #if full training set = 17770, else the number of training files you're using
	ratings_rdd,probe_rdd,test_data = loadFiles(ratings_dir,limit)
	model = findBestModel(ratings_rdd, test_data, probe_rdd)

	sc.stop()
