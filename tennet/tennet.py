import logging
import custom_logstash
import helper
import settings
from time import sleep
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = custom_logstash.LogstashHandler('logstashhost', 9393, ssl=False)
formatter = custom_logstash.LogstashFormatter()
handler.setFormatter(formatter)
logger.addHandler(handler)


# TODO add more specific exception classes and remove the generic exception handling.
def run():
    # Loop continuously until there is any data and if there is new data return
    try:
        while True:
            date_range = helper.get_date_range(datetime.now())
            logging.info("tennet: Finished creating the date range")
            base_url = helper.create_tennet_url(date_range)
            logging.info("tennet: Finished creating the URL to get the data from")
            raw_data = helper.parse_tennet_url(base_url)
            logging.info("tennet: Finished scraping the data")
            if raw_data.shape[0] == 0:
                sleep(settings.SLEEPER_TIME)
                print("Waiting for data")
            else:
                parsed_data = helper.parse_df(raw_data)
                logging.info("tennet: Finished parsing the data")
                helper.produce_msg_to_kafka(settings.BOOTSTRAP_SERVER, settings.KAFKA_TOPIC, parsed_data)
            return
    except Exception as err:
        logging.error("tennet: Execution error")


if __name__ == '__main__':
    errors = 0
    scheduler = BlockingScheduler(max_instances=1)
    scheduler.add_job(run, 'cron', hour='8', day_of_week='mon,tue,wed,thu,fri',
                      max_instances=1, misfire_grace_time=120)
    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("Stopped by keyboard interrupt")
        raise KeyboardInterrupt
    except SystemExit:
        raise SystemExit
