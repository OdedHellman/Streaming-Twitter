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
    def __init__(self, stream_rule, project_id, topic_id, topic_path):
        # Using env var to avoid hardcoding credentials
        super().__init__(bearer_token=environ['TWITTER_API_BEARER'])
        self.stream_rule = stream_rule
        self.project_id = project_id
        self.topic_id = topic_id
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
    project_id = config['project']['project_id']
    topic_id = config['project']['topic_id']
    topic_path = config['project']['topic_path']

    streaming_client = Client(stream_rule, project_id, topic_id, topic_path)
    
    # Delete previous rules -> Twitter "Essential" API only allows 1 rule :( 
    rules = streaming_client.get_rules().data
    if rules:
        print("Deleting previous rules...")
        existing_rules = [rule.id for rule in streaming_client.get_rules().data]
        streaming_client.delete_rules(ids=existing_rules)

    # Add the rule and run the stream
    streaming_client.add_rules(tweepy.StreamRule(stream_rule))
    streaming_client.filter(tweet_fields=tweet_fields,
                            user_fields=user_fields
                            )

if __name__ == "__main__":
    main()
    