import configparser
import json
import warnings
warnings.filterwarnings("ignore")

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.options.pipeline_options import GoogleCloudOptions


class SetValues(beam.DoFn):
    """A simple DoFn that sets the values for the output table."""

    @staticmethod
    def process(element, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime().strftime("%d-%m-%Y, %H:%M:%S")
        
        # instead return an iterable holding the output.
        # We can emitting individual elements with a yield statement.
        yield {'language': element.lang,
               'count': element.count,
               'timestamp': window_start
               }

class SetUpper(beam.DoFn):
    """A simple DoFn that Upper 'language' field"""

    @staticmethod
    def process(element):
        element['language'] = str.upper(element['language'])

        # instead return an iterable holding the output.
        # We can emitting individual elements with a yield statement.
        yield element
        

def run():
    # Parse config file (can use argparse instead)
    config = configparser.ConfigParser()
    config.read('./config/config.ini')
    
    dataset_name = config['bigquery']['dataset_name']
    tweets_table = config['bigquery']['tweets_table']
    agg_table = config['bigquery']['agg_table']
    project_id = config['gcp']['project_id']
    topic_path = config['gcp']['topic_path']
    window_size = int(config['project']['WINDOW_SIZE'])
    
    options = PipelineOptions(flags=[project_id, topic_path], save_main_session=True, streaming=True)
    options.view_as(GoogleCloudOptions).project = project_id

    # Setting up the tables schemas
    agg_schema = json.load(open("./schemas/agg.json"))
    tweet_schema = json.load(open("./schemas/tweet.json"))
    #print(type(agg_schema).__name__)

    # Pipeline setup
    with beam.Pipeline(options=options) as pipeline:

        # Read from PubSub and parse the data
        tweets = ( pipeline
                | "Read from PubSub" >> beam.io.ReadFromPubSub(topic_path)
                | "Parse json object" >> beam.Map(lambda x: json.loads(x.decode("utf-8")))
                )

        # Write tweets to BigQuery table
        tweets | "Write back raw data to BigQuery" >> beam.io.WriteToBigQuery(
            tweets_table,
            dataset=dataset_name,
            project=project_id,
            schema=tweet_schema
        )

        # Write back tweets by window, after aggregation
        (tweets
            | "Set Windows size" >> beam.WindowInto(beam.window.FixedWindows(window_size))
            | "Aggregation for each language" >> beam.GroupBy(lang=lambda x: x["lang"])
                                            .aggregate_field(lambda x: x["lang"],
                                                            CountCombineFn(),
                                                            'count'
                                                            )
            | "Set timestamp and reformat output" >> beam.ParDo(SetValues())
            | "Set language field to UpperCase" >> beam.ParDo(SetUpper())
            | "Write back aggregated data to BigQuery" >> beam.io.WriteToBigQuery(
                agg_table,
                dataset=dataset_name,
                project=project_id,
                schema=agg_schema
            )
        )

        # Keep the pipeline running
        pipeline.run().wait_until_finish()


if __name__ == "__main__":
    run()
