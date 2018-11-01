#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from pyspark import SparkContext, SparkConf

def create_mutual_friends(line):
    
    person = line[0].strip()
    friends = line[1]
    if(person != ''):
        person = int(person)
        
        final_friend_values = []
        
        for friend in friends:
            
            friend = friend.strip()
            
            if(friend != ''):
                friend = int(friend)
                if(int(friend) < int(person)):
                    val = (str(friend)+","+str(person),set(friends))
                else:
                    val = (str(person)+","+str(friend),set(friends))
                
                final_friend_values.append(val)
        return(final_friend_values)

def final_map(line):
    
    key = line[0]
    value = list(line[1])
    string = ",".join(value)
    return("{0}\t {1}".format(key,string))

if __name__ == "__main__":
    config = SparkConf().setAppName("mutualfriends").setMaster("local[2]")
    sc = SparkContext(conf = config)
    
    mutual_friends = sc.textFile("soc-LiveJournal1Adj.txt")
    
    lines_split = mutual_friends.map(lambda x : x.split("\t")).filter(lambda x : len(x) == 2).map(lambda x: [x[0],x[1].split(",")])
    
    mutual_friends1 = lines_split.flatMap(create_mutual_friends)
    
    reducerdd = mutual_friends1.reduceByKey(lambda x,y: x.intersection(y))
    
    finalrdd = reducerdd.map(final_map)
    
    finalrdd.coalesce(1).saveAsTextFile("output.txt")