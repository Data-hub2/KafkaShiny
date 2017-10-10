# Base URL to download the imbalance prices data from
BASE_URL= "http://www.tennet.org/english/operational_management/export_data.aspx?exporttype=settlementprices"
# The format to download the data
TYPE = "&format=csv"
# The submit format to use
SUBMIT_TYPE = "&submit=1"
# The columns required from the data
COLUMNS = ["Date", "PTE", "To regulate up", "To regulate down", "Consume", "Feed", "Regulation state"]
# The column mapping from source to kafka
COLUMN_MAPPING = {"Date": "date", "PTE": "ptu", "To regulate up": "upward_dispatch",
                  "To regulate down": "downward_dispatch", "Consume": "take_from_system",
                  "Feed": "feed_into_system", "Regulation state": "regulation_state"}
# Kafka Bootstrap server settings
BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = "tennet"
# Sleep time
SLEEPER_TIME = 90
