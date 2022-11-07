import json
import configparser

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.transforms.combiners import CountCombineFn

WINDOW_SIZE = 30 # number of second for aggregation window

class SetValues(beam.DoFn):
    """A DoFn that sets the values for the output table."""

    @staticmethod
    def process(element, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime().strftime("%d-%m-%Y, %H:%M:%S")
        
        # instead return an iterable holding the output.
        # We can emitting individual elements with yield statement.
        yield {'language': str.upper(element.lang),
               'count': element.count,
               'timestamp': window_start
               }


def run():
    # Parse config file (can use argparse instead)
    config = configparser.ConfigParser()
    config.read('./config/config.ini')
    
    dataset_name = config['project']['dataset_name']
    tweets_table = config['project']['tweets_table']
    project_id = config['project']['project_id']
    agg_table = config['project']['agg_table']
    topic_path = config['project']['topic_path']
    
    options = PipelineOptions(flags=[project_id, topic_path], save_main_session=True, streaming=True)
    options.view_as(GoogleCloudOptions).project = project_id

    # Setting up the tables schemas
    agg_schema = json.load(open("./schemas/agg.json"))
    tweet_schema = json.load(open("./schemas/tweet.json"))
    #print(type(agg_schema).__name__)

    # Pipeline setup
    pipeline = beam.Pipeline(options=options)

    tweets = (
        pipeline | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=topic_path)
                 | "Parse json object" >> beam.Map(lambda element: json.loads(element.decode("utf-8")))
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
        | "Set Windows size" >> beam.WindowInto(beam.window.FixedWindows(WINDOW_SIZE))
        | "Aggregation for each language" >> beam.GroupBy(lang=lambda x: x["lang"])
                                          .aggregate_field(lambda x: x["lang"],
                                                           CountCombineFn(),
                                                           'count'
                                                           )
        | "Sets TimeStamp and reformat output" >> beam.ParDo(SetValues())
        | "Write back agg data to BigQuery" >> beam.io.WriteToBigQuery(
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
