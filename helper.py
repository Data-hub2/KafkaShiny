import settings
import logging
import json
import hashlib
import pandas as pd
from datetime import datetime, timedelta
from pykafka import KafkaClient


def parse_tennet_url(date_range):
    """
    Parse the tennet imbalance Endpoint to get the data
    :param date_range: The date range to the data for
    :type date_range: string
    :return: The Dataframe for the extracted data
    :rtype: Dataframe Object
    """
    logging.info("tennet: Started getting the data")
    number_of_retries = 0
    base_url = settings.BASE_URL+settings.TYPE+date_range+settings.SUBMIT_TYPE
    df = pd.DataFrame()
    try:
        df_temp = pd.read_csv(base_url).reset_index(drop=True)
        df = pd.concat([df, df_temp], axis=1)
    except ValueError:
        number_of_retries += 1
        print("Retrying, #{}".format(number_of_retries))
    return df


def get_date_range():
    curr_day_of_week = datetime.today().weekday()
    curr_date = datetime.now()
    prev_date = (curr_date - timedelta(1)).strftime("%d-%m-%Y")
    # If the current day is monday, then we need to get the data for fri,sat and sun.
    if curr_day_of_week == 0:
        last_fri_date = (curr_date - timedelta(3)).strftime("%d-%m-%Y")
        date_range = "&datefrom="+last_fri_date+"&dateto="+prev_date
    else:
        date_range = "&datefrom="+prev_date+"&dateto="+prev_date
    return date_range


def parse_df(raw_data):
    """
    Parse the raw_data JSON and create a formatted JSON
    :param raw_data: The raw JSON from OWM
    :type raw_data: String
    :return: The formatted JSON
    :rtype: JSON Object
    """
    logging.info("tennet: Started parsing the data")
    # Convert the raw dataframe to a formatted JSON with all the required fields
    df = raw_data.filter(items=settings.COLUMNS)
    df = df.rename(columns=settings.COLUMN_MAPPING)
    formatted_json = df.to_json(orient="records")
    return formatted_json


def produce_msg_to_kafka(bootstrap_server, topic, message):
    """
    Produce the input message to the given kafka topic
    :param message: JSON array containing the messages
    :type message: JSON String
    :param bootstrap_server: The location of the kafka bootstrap server
    :type bootstrap_server: String
    :param topic: The topic to which the message is produced
    :type topic: String
    """
    logging.info('tennet: Producing message to Kafka')
    # Setup the kafka producer
    client = KafkaClient(bootstrap_server)
    topic = client.topics[topic.encode()]
    producer = topic.get_producer(sync=True)
    records = json.loads(message)
    for record in records:
        # print(json.dumps(record).encode())
        hash_object = hashlib.md5(json.dumps(record).encode()).hexdigest()
        record = record.update({'uid': hash_object})
        producer.produce(json.dumps(record).encode())
    logging.info('tennet: Finished producing message to Kafka')
