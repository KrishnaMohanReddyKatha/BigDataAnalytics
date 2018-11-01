find_mutual_friends.py:
			Spark job to find mutual friends given a person and his friends data. Data file used: soc-LiveJournal1Adj.txt

find_top_ten_mutual_friends_with_friendsInfo.py:
			Find top-10 friend pairs by their total number of common friends. For each top-10 friend pair, print detailed information in decreasing order of total number of common friends. 
			DataFiles used : soc-LiveJournal1Adj.txt, Userdata.txt

movieRatings.py:
			Spark job
			             To calculate the average ratings of each movie.
						 Give the names of bottom 10 movies with lowest average ratings.
						 Find average rating of each movie where the movie’s tag is ‘action’.
						 Find average rating of each movie where the movie’s tag is ‘action’ and  genre contains ‘thrill’.