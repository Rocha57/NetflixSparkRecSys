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
		print("Rank "+str(rank)+" and Iterations "+str(num_iterations)+" Model already exists, loading...")
		model = MatrixFactorizationModel.load(sc, save_file)
	else:
		print("Model does not exist, training ALS with rank "+str(rank)+" and "+str(num_iterations)+" iterations")
		model = ALS.train(data, rank, num_iterations)
		print("Saving new model")
		model.save(sc,save_file)
	return model

def calculateAccuracy(predictions_vs_ratings):
	right = 0
	for line in predictions_vs_ratings.collect():
		if line[1][0] == round(line[1][1]):
			right += 1

	#print("Right predictions: "+str(right))
	total = predictions_vs_ratings.count()
	#print("Total predictions: "+str(total))

	accuracy = (right/float(total)) * 100
	return round(accuracy,2)


def computeRMSE(model, test_data, real_data):
	predictions = model.predictAll(test_data).map(lambda r: ((r[0], r[1]), r[2]))
	probe_adaptado = real_data.map(lambda r: ((r[0], r[1]), r[2]))
	predictions_vs_ratings = probe_adaptado.join(predictions)
	RMSE = sqrt(predictions_vs_ratings.map(lambda r:  (r[1][0] - r[1][1])**2).reduce(lambda x, y: x + y)/predictions_vs_ratings.count())
	print("RMSE: "+str(RMSE))
	accuracy = calculateAccuracy(predictions_vs_ratings)
	print("Accuracy: "+str(accuracy)+"%")
	return (RMSE, predictions_vs_ratings)

def findBestModel(train_data, test_data, real_data):
	ranks = [4,5,6,7,9,11]
	num_iterations = 5
	best_RMSE = float("inf")
	previous_RMSE = float("inf")
	best_rank = -1
	best_num_iterations = -1
	for rank in ranks:
		model = trainModel(train_data,rank,num_iterations)
		RMSE, predictions_vs_ratings = computeRMSE(model, test_data, real_data)
		if RMSE > previous_RMSE:
			best_RMSE = previous_RMSE
			best_rank = previous_rank
			best_num_iterations = num_iterations
			best_model = previous_model
			best_predictions = predictions_vs_ratings
			break #Comment this line if you want to test all rankings instead of stopping when the best one is found
		previous_rank = rank
		previous_model = model
		previous_RMSE = RMSE 
		previous_predictions = predictions_vs_ratings
	print("Best model is with rank "+str(best_rank)+" and "+str(num_iterations)+" iterations with a RMSE = "+str(best_RMSE))
	return (best_model, best_predictions)


if __name__ == "__main__":
	sc = SparkContext("local", "main") #Standalone version
	ratings_dir = "/Users/Rocha/Documents/Datasets/download/train1500" #Standalone version, 1500 training movie files
	#ratings_dir = "/Users/Rocha/Documents/Datasets/download/training_set" #Full training dataset to be used in scalable version
	limit = 1500 #if full training set = 17770, else the number of training files you're using
	ratings_rdd,probe_rdd,test_data = loadFiles(ratings_dir,limit)
	model, predictions_vs_ratings_rdd = findBestModel(ratings_rdd, test_data, probe_rdd)


	sc.stop()
