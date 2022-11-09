import json
import configparser
import tweepy
from google.cloud.pubsub_v1 import PublisherClient

SEP = '-' * 100

class Client(tweepy.StreamingClient):
    """
    Client class for handling Twitter API responses.
    """
    def __init__(self, stream_rule, topic_path, bearer_token):
        try:
            super().__init__(bearer_token=bearer_token)
        except ValueError as e:
            print(f'Something went wrong with Twitter Bearer Token \n{e}')
            exit(1)
            
        self.stream_rule = stream_rule
        self.topic_path = topic_path
        self.publisher = PublisherClient()
        
    def on_response(self, response):
        """
        Override the default on_response method to handle the response.
        """
        data = response.data.data
        data["stream_rule"] = self.stream_rule
        data_formatted = json.dumps(data).encode("utf-8")
        print(data_formatted, '\n', SEP)
        
        # publish to pubsub
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
    bearer_token = config['twitter']['bearer_token']

    stream = Client(stream_rule, topic_path, bearer_token)
    
    # Delete previous rules -> Twitter "Essential" API only allows 1 rule :( 
    rules = stream.get_rules().data
    if rules:
        print("Deleting previous rules...")
        stream.delete_rules(
            [rule.id for rule in stream.get_rules().data]
            )

    # Add the rule and run the stream
    stream.add_rules(tweepy.StreamRule(stream_rule))
    stream.filter(tweet_fields=tweet_fields,
                  user_fields=user_fields)

if __name__ == "__main__":
    main()
    