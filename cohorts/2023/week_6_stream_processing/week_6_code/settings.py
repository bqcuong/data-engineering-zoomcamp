import pyspark.sql.types as T

FHV_INPUT_DATA_PATH = 'data/fhv_tripdata_2019-02.csv.gz'
GREEN_INPUT_DATA_PATH = 'data/green_tripdata_2019-01.csv.gz'

TOPIC_WINDOWED_VENDOR_ID_COUNT = 'vendor_counts_windowed'

FHV_PRODUCE_TOPIC_RIDES = FHV_CONSUME_TOPIC_RIDES = 'fhv_rides'
GREEN_PRODUCE_TOPIC_RIDES = GREEN_CONSUME_TOPIC_RIDES = 'green_rides'

RIDE_SCHEMA = T.StructType(
    [T.StructField("vendor_id", T.IntegerType()),
     T.StructField("PULocationID", T.IntegerType()),
     T.StructField("DOLocationID", T.IntegerType()),
     T.StructField('tpep_pickup_datetime', T.TimestampType()),
     T.StructField('tpep_dropoff_datetime', T.TimestampType()),
     T.StructField("passenger_count", T.IntegerType()),
     T.StructField("trip_distance", T.FloatType()),
     T.StructField("payment_type", T.IntegerType()),
     T.StructField("total_amount", T.FloatType()),
     ])

def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf