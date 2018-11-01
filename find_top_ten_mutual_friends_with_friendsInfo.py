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



def cal_length(line):
    
    key = line[0]
    value = len(line[1])
    
    return(key,value)

def split_user(line):
    
    friend_pair = line[0]
    no_of_friends = line[1]
    
    friend_pair = friend_pair.split(",")
    
    return(friend_pair[0],(friend_pair[1],no_of_friends))

def userdata_format(line):
    
    line = line.split(",")
    
    return(line[0],(line[1],line[2],line[3]))

def user_data_format1(line):
    
    user1 = line[0]
    user2 = line[1][0][0]
    no_of_friends = line[1][0][1]
    user1_data = line[1][1]
    
    return( user2,((user1,no_of_friends),user1_data) ) 

def final_map(line):
    
    no_of_friends = str(line[1][0][0][1])
    user1_data = line[1][0][1]
    user2_data = line[1][1]
    
    user1_firstname = user1_data[0]
    user1_lastname = user1_data[1]
    user1_address = user1_data[2]
    
    user2_firstname = user2_data[0]
    user2_lastname = user2_data[1]
    user2_address = user2_data[2]
    
    return("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}".format(no_of_friends,user1_firstname,user1_lastname,user1_address,user2_firstname,user2_lastname,user2_address))

if __name__ == "__main__":
    config = SparkConf().setAppName("mutualfriends").setMaster("local[2]")
    sc = SparkContext(conf = config)
    mutual_friends = sc.textFile("soc-LiveJournal1Adj.txt")
    lines_split = mutual_friends.map(lambda x : x.split("\t")).filter(lambda x : len(x) == 2).map(lambda x: [x[0],x[1].split(",")])
    mutual_friends1 = lines_split.flatMap(create_mutual_friends)
    reducerdd = mutual_friends1.reduceByKey(lambda x,y: x.intersection(y))
    lengthrdd = reducerdd.map(cal_length)
    sortedmap = lengthrdd.takeOrdered(10, key = lambda x: -x[1])
    sortedmapper = sc.parallelize(sortedmap)
    friends1 = sortedmapper.map(split_user)
    userdata = sc.textFile("userdata.txt")
    formatedddata = userdata.map(userdata_format)
    joinrdd = friends1.join(formatedddata)
    
    joinrdd_format = joinrdd.map(user_data_format1)
    
    joinrdd1 = joinrdd_format.join(formatedddata)
    
    finalrdd = joinrdd1.map(final_map)
    
    finalrdd.coalesce(1).saveAsTextFile("q2_output.txt")