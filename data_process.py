import argparse
import json
import configparser

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.transforms.combiners import CountCombineFn

WINDOW_SIZE = 60 # number of second setting the timestamp aggregation window

class SetValues(beam.DoFn):
    """A DoFn that sets the values for the output table."""
    
    @staticmethod
    def process(element, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime().strftime("%d-%m_%H:%M")
        yield {'language': str.upper(element.lang),
               'count': element.count,
               'timestamp': window_start,
               }

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--pid',
                        type=str,
                        required=True,
                        help='project id as shown in the GCP console'
                        )
    
    parser.add_argument('--topic_path',
                        type=str,
                        required=True,
                        help='projects/<pid>/topics/<BQ_DATABASE_NAME>'
                        )

    return parser.parse_known_args()


def run():
    # Parse config file
    config = configparser.ConfigParser()
    config.read('./config/config.ini')
    
    dataset_name = config['project']['dataset_name']
    tweets_table = config['project']['tweets_table']
    project_id = config['project']['project_id']
    agg_table = config['project']['agg_table']
    
    # Setting up the Beam pipeline options
    args, pipeline_args = parse_args()
    options = PipelineOptions(pipeline_args, save_main_session=True, streaming=True)
    options.view_as(GoogleCloudOptions).project = project_id

    # Setting up the tables schemas
    agg_schema = json.load(open("./tableschemas/agg.json"))
    tweet_schema = json.load(open("./tableschemas/tweet.json"))
    #print(type(agg_schema).__name__)

    # Pipeline
    pipeline = beam.Pipeline(options=options)

    raw_tweets = (pipeline | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=args.topic_path)
                           | "ParseJson" >> beam.Map(lambda element: json.loads(element.decode("utf-8")))
                           )

    # Write tweets to BigQuery table
    raw_tweets | "Write raw to bigquery" >> beam.io.WriteToBigQuery(
        tweets_table,
        dataset=dataset_name,
        project=project_id,
        schema=tweet_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    )

    # Write back tweets by window, after aggregation
    (raw_tweets
        | "Window" >> beam.WindowInto(beam.window.FixedWindows(WINDOW_SIZE))
        | "Aggregate per language" >> beam.GroupBy(lang=lambda x: x["lang"])
                                          .aggregate_field(lambda x: x["lang"],
                                                           CountCombineFn(),
                                                           'count'
                                                           )
        | "Add Timestamp" >> beam.ParDo(SetValues())
        | "Write agg to bigquery" >> beam.io.WriteToBigQuery(
            agg_table,
            dataset=dataset_name,
            project=args.pid,
            schema=agg_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
     )

    pipeline.run().wait_until_finish()


if __name__ == "__main__":
    run()