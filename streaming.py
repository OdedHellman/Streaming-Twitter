import json
from os import environ
import configparser

import tweepy
from google.cloud import pubsub_v1

SEP = '-'*100

class Client(tweepy.StreamingClient):
    """
    Client class for handling Twitter API responses.
    """
    def __init__(self, stream_rule, topic_path):
        # use env var to avoid hardcoding credentials
        super().__init__(bearer_token=environ['TWITTER_API_BEARER'])
        self.stream_rule = stream_rule
        self.topic_path = topic_path
        self.publisher = pubsub_v1.PublisherClient()
        
    def on_response(self, response):
        data = response.data.data

        # Write to pubsub
        data["stream_rule"] = self.stream_rule
        data_formatted = json.dumps(data).encode("utf-8")
        print("Streaming: ", data_formatted, '\n', SEP)
        self.publisher.publish(data=data_formatted,
                               topic=self.topic_path
                               )
        
def main():
    # Response fields
    tweet_fields = ['id', 'text', 'author_id', 'created_at', 'lang']
    user_fields = ['description', 'created_at', 'location']
    
    # Parse config file (can use argparse instead)
    config = configparser.ConfigParser()
    config.read('./config/config.ini')
    
    stream_rule = config['project']['rule']
    topic_path = config['gcp']['topic_path']

    stream = Client(stream_rule, topic_path)
    
    # Delete previous rules -> Twitter "Essential" API only allows 1 rule :( 
    rules = stream.get_rules().data
    if rules:
        print("Deleting previous rules...")
        existing_rules = [rule.id for rule in stream.get_rules().data]
        stream.delete_rules(ids=existing_rules)

    # Add the rule and run the stream
    stream.add_rules(tweepy.StreamRule(stream_rule))
    stream.filter(tweet_fields=tweet_fields,
                  user_fields=user_fields)

if __name__ == "__main__":
    main()
    