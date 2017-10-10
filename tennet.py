import settings
import logging
import helper
from time import sleep
from custom_logstash import LogstashFormatter
from custom_logstash import LogstashHandler
from apscheduler.schedulers.blocking import BlockingScheduler

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = LogstashHandler('logstashhost', 9393, ssl=False)
formatter = LogstashFormatter()
handler.setFormatter(formatter)
logger.addHandler(handler)


def run():
    # Loop continuously until there is any data and if there is new data return
    while True:
        logging.info("tennet: Get the date range to get the data for")
        date_range = helper.get_date_range()
        raw_data = helper.parse_tennet_url(date_range)
        logging.info("tennet: Finished scraping the data")
        if raw_data.shape[0] == 0:
            sleep(settings.SLEEPER_TIME)
            print("Waiting for data")
        else:
            parsed_data = helper.parse_df(raw_data)
            logging.info("tennet: Finished parsing the data")
            helper.produce_msg_to_kafka(settings.BOOTSTRAP_SERVER, settings.KAFKA_TOPIC, parsed_data)
            return


if __name__ == '__main__':
    errors = 0
    scheduler = BlockingScheduler(max_instances=1)
    scheduler.add_job(run, 'cron', hour='8', day_of_week='mon,tue,wed,thu,fri',
                      max_instances=1, misfire_grace_time=None)
    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("Stopped by keyboard interrupt")
        raise KeyboardInterrupt
    except SystemExit:
        raise SystemExit
    except:
        errors += 1
        sleep(100)
