#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from pyspark import SparkContext, SparkConf
import csv

def movies_format(line):
    return(line[0],(line[1],line[2].split("|")))

def ratings_format(line):

    return(line[1],(line[0],line[2],line[3]))

def tags_format(line):
    return(line[1],(line[0],line[2],line[3]))

def compute_average(line):
    return( sum(line)/len(line))

def join_rdd(line):
    return("{0}\t{1}".format(line[1][1][0], str(line[1][0]) ))

if __name__ == "__main__":
    
    config = SparkConf().setAppName("mutualfriends").setMaster("local[2]")
    
    sc = SparkContext(conf = config)
    
    movies = sc.textFile("movies.csv")
    
    ratings = sc.textFile("ratings.csv")
    
    tags = sc.textFile("tags.csv")
    
    movies = movies.mapPartitions(lambda x: csv.reader(x))
    ratings = ratings.mapPartitions(lambda x: csv.reader(x))
    tags = tags.mapPartitions(lambda x: csv.reader(x))
    
    movies_rdd= movies.map(movies_format).filter(lambda x: x[0] != 'movieId')
    ratings_rdd = ratings.map(ratings_format).filter(lambda x:x[0] != 'movieId').map(lambda x: (x[0],float(x[1][1])) )
    tags_rdd = tags.map(tags_format).filter(lambda x: x[0] != 'movieId')
    
    
    ratings_group_rdd = ratings_rdd.mapValues(lambda v: (v,1)).reduceByKey(lambda a,b: (a[0] + b[0],a[1] + b[1]) ).mapValues(lambda v: v[0]/v[1])
    
    
    finalrdd = ratings_group_rdd.map(lambda x: "{0}\t{1}".format(x[0],str(x[1])))
    
    finalrdd.coalesce(1).saveAsTextFile("average_ratings1.txt")
    
    sortedmap = ratings_group_rdd.takeOrdered(10, key = lambda x: x[1])
    sortedmapper = sc.parallelize(sortedmap)
    
    joinrdd = sortedmapper.join(movies_rdd)
    
    joinrdd_movies = joinrdd.map(join_rdd)
    
    joinrdd_movies.coalesce(1).saveAsTextFile("lowest_ratings.txt")
    
    action_movie_rdd = tags_rdd.filter(lambda x: x[1][1] == 'action')
    
    action_movie_average_rdd = ratings_group_rdd.join(action_movie_rdd)
    
    action_movie_average_rdd1 = action_movie_average_rdd.map(lambda x: "{0}\t{1}".format(x[0],str(x[1][0])))
    action_movie_average_rdd1.coalesce(1).saveAsTextFile("actions_average.txt")
   
    thriller_movies_rdd = movies_rdd.filter(lambda x: "Thriller" in x[1][1])
    
    action_movies_ratings = action_movie_average_rdd.map(lambda x: (x[0],x[1][0]))
    
    action_thriller_movies = action_movies_ratings.join(thriller_movies_rdd)
    
    action_thriller_movies = action_thriller_movies.map(lambda x: "{0}\t{1}".format(x[0],str(x[1][0])))
    
    action_thriller_movies.coalesce(1).saveAsTextFile("action_thriller.txt")