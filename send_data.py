import json
import sys
import requests
import requests_oauthlib
import tweepy
import logging
import pickle
import socket

# Deserialize the TCP connection object
serialized_conn = sys.argv[1]
conn = pickle.loads(serialized_conn)

# Function to send tweets to the TCP connection
def send_tweets_to_spark(http_resp, tcp_connection):
    # Iterate over the response lines from the Twitter API
    for line in http_resp.iter_lines():
        try:
            # Parse each line as JSON
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            
            # Print the tweet text and send it to the TCP connection
            print("Tweet Text: " + tweet_text)
            print("------------------------------------------")
            tcp_connection.send(tweet_text.encode() + b'\n')
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

# Get tweets from the Twitter API
response = get_tweets()

# Send the tweets to the TCP connection
send_tweets_to_spark(response, conn)
    
# Function to get tweets from the Twitter API
def get_tweets():
    # Set the URL and query parameters for filtering tweets
    url = 'https://api.twitter.com/1.1/search/tweets.json'
    query_params = {
        'q': 'ios OR apple OR AAPL OR iphone OR ipad',
        'lang': 'en',
        'result_type': 'recent',
        'count': 1500
    }

    # Create an OAuth1 authentication object
    my_auth = requests_oauthlib.OAuth1(consumer_key, consumer_secret, access_token, access_token_secret)

    # Send the request to the Twitter API with authentication
    response = requests.get(url, params=query_params, auth=my_auth)

    return response

# Function to process the response and extract tweet information
def process_tweets(response):
    if response.status_code == 200:
        json_response = response.json()
        tweets = json_response.get('statuses', [])
        
        for tweet in tweets:
            tweet_id = tweet['id']
            created_at = tweet['created_at']
            author_id = tweet['user']['id_str']
            tweet_text = tweet['text']
            
            # Print the tweet details
            print('Tweet ID:', tweet_id)
            print('Created At:', created_at)
            print('Author ID:', author_id)
            print('Tweet Text:', tweet_text)
            print('--------------------------')
    else:
        print('Error:', response.status_code)

# Function to send tweets to the TCP connection
def send_tweets_to_spark(http_resp, tcp_connection):
    # Iterate over the response lines from the Twitter API
    for line in http_resp.iter_lines():
        try:
            # Parse each line as JSON
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            
            # Print the tweet text and send it to the TCP connection
            print("Tweet Text: " + tweet_text)
            print("------------------------------------------")
            tcp_connection.send(tweet_text.encode() + b'\n')
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

# Get tweets from the Twitter API
response = get_tweets()

# Process the response and extract tweet information
process_tweets(response)

# Send the tweets to the TCP connection
send_tweets_to_spark(response, conn)