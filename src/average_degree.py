from datetime import datetime
import json
import os
import numpy as np
import string
from sys import argv
import time
from collections import defaultdict
import networkx as nx

def parse_tweet(line):
	'''This function will parse the Json format 
	tweet and returns two fields "created_at" and "hashtags" as a list '''
	tweet = json.loads(line)    
    	if tweet['lang'] != 'en':
     		return ['',[]]
	date = tweet["created_at"]
    	hashtags = [hashtag["text"] for hashtag in tweet["entities"]["hashtags"]]
    	return [date, hashtags]    
    
def extract_hashtag_list(tweet):
	''' Just a helper function to extract the hashtags list from the parsed_tweet list''' 
	tw = parse_tweet(tweet)[1]
	return tw

def extract_timestamp(tweet):
	current_tweet_time = []
	current_tweet_time = parse_tweet(tweet)[0]
	return current_tweet_time

def compare_time(current_time,parsed_time):
	''' This function helps to compare the times of two tweets to check 
		they fall within 60sec sliding window'''
	if not current_time or not parsed_time :
		return False
	m_time =  datetime.strptime(parsed_time,'%a %b %d %H:%M:%S +0000 %Y')
    	c_time =  datetime.strptime(current_time,'%a %b %d %H:%M:%S +0000 %Y')
	difference_time = c_time-m_time
	if(difference_time.total_seconds() <= 60):
		return True
	else:
		return False

def parse_tweets_stream(input_tweet_stream):
	''' This function takes raw input stream of tweets and processes 
		them to tweets with only two fields: created_at and hashtags '''
	tweets = []
	for line in input_tweet_stream:
	        tweet = json.loads(line)
	        if not("created_at" in line or "text" in line):
	            continue
	        tweet = {}
		tweet["created_at"] = parse_tweet(line)[0]
		tweet["hashtags"] = parse_tweet(line)[1]		
	        tweets.append(tweet)
    	return tweets


def average_degree(tweets):
	''' Calculates the average degree of the graph'''
	''' uses "networkx" module for creating and updating graphs'''
	current_graph = nx.Graph()
	print len(tweets)
	i = 0
	j = 0
	for i in range(len(tweets)-1):
		for j in range(i):
			if(not compare_time(tweets[j]["created_at"],tweets[i]["created_at"])):
           			if len(tweets[j]["hashtags"]) >= 2:					
					for k in range(len(tweets[j]["hashtags"])):
						if vertex_present(current_graph,tweets[j]["hashtags"][k]):
							current_graph.remove_node(tweets[j]["hashtags"][k])
			

		if len(tweets[i]["hashtags"]) >= 2:
			for k in range(len(tweets[i]["hashtags"])):
				for l in range(len(tweets[i]["hashtags"])):
					current_graph.add_edge(tweets[i]["hashtags"][k],tweets[i]["hashtags"][l])
		#j = j+1
		
		
		
	avg_degree_of_graph = 0.00
	avg_degree = []
	total_degree = 0
	total_number_of_nodes = number_of_nodes(current_graph)	
	nodes_in_graph = current_graph.nodes()
	if(total_number_of_nodes is None):
		average_degree.append(format(0.00,'.2f'))
	for i in range(len(nodes_in_graph)):
		total_degree = total_degree + current_graph.degree(nodes_in_graph[i])
	average_degree_of_graph = total_degree / float(total_number_of_nodes)
	average_degree.append(format(round(average_degree_of_graph,2),'.2f'))		
	return average_degree

def degree_of_vertex(current_graph,current_vertex):
	''' Uses networkx to return the degree of a vertex in a graph'''
	degree = 0
	degree = current_graph.degree(current_vertex)
	return degree
def number_of_nodes(graph):
	return graph.number_of_nodes()

def vertex_present(current_graph, vertex):
	''' This function enables us to check if a hashtag of a new tweet is already
		in the graph or not '''
	vertex_list = current_graph.nodes()
	for i in range(len(vertex_list)):
		if(vertex_list[i] == vertex):
			return True
		else:
			return False

if __name__ == "__main__":
	
	#efficient line-by-line read of big files
	# Reads from input file and writes to output file
	script, filename = argv
	input_tw = []	
	with open(filename) as ip:
		input_tw = ip.readlines()
	
	input_tweets = parse_tweets_stream(input_tw)
	avg_degree = average_degree(input_tweets)
	with open("output_tweets.txt","w") as output:
		for line in avg_degree:
			output.write(str(line) + "\n")							
	output.flush()
 	ip.close()
 	output.close()



