import pandas as pd

from influxdb_client import InfluxDBClient
from influxdb_client import PostBucketRequest
from influxdb_client.rest import ApiException
from influxdb_client import Point
from influxdb_client import WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

class InfluxDbTool():

    #
    # constructor
    #
    def __init__(
            self,
            INFLUXDB_URL : str,
            INFLUXDB_TOKEN : str,
            INFLUXDB_ORG : str,
            timeout : int = 120000,
    ):
        self.INFLUXDB_URL = INFLUXDB_URL
        self.INFLUXDB_TOKEN = INFLUXDB_TOKEN
        self.INFLUXDB_ORG = INFLUXDB_ORG
        self.client = InfluxDBClient(url=self.INFLUXDB_URL, token=self.INFLUXDB_TOKEN, org=self.INFLUXDB_ORG, timeout = timeout)

    #
    # deconstructor
    #
    def __del__(self):
        self.client.close()

    #
    # create a bucket
    #
    def create_bucket(self, bucket_name : str):
        buckets_api = self.client.buckets_api()
        org = self.client.organizations_api().find_organizations(org = self.INFLUXDB_ORG)[0]
        org_id = org.id

        try:
            # Check if the bucket already exists
            existing_bucket = buckets_api.find_bucket_by_name(bucket_name)
            if existing_bucket:
                print(f"Bucket '{bucket_name}' already exists.")
            else:
                # Create a new bucket
                bucket_req = PostBucketRequest(
                    org_id = org_id,
                    name = bucket_name,
                )
                bucket = buckets_api.create_bucket(bucket = bucket_req)
                print("Created bucket:", bucket.name, "schema_type:", bucket.schema_type)

        except ApiException as e:
            print(f"Error creating bucket: {e}")

    #
    # delete a bucket
    #
    def delete_bucket(self, bucket_name : str):
        buckets_api = self.client.buckets_api()
        buckets = buckets_api.find_buckets().buckets

        try:
            # Find the target bucket
            target_bucket = None
            for bucket in buckets:
                if bucket.name == bucket_name:
                    target_bucket = bucket
                    buckets_api.delete_bucket(target_bucket.id)
                    print(f"Bucket '{bucket_name}' deleted successfully.")
                    break

            if not target_bucket:
                print(f"Bucket '{bucket_name}' not found.")
        except ApiException as e:
            print(f"Error deleting bucket: {e}")

    #
    # Convert a column of timestamps (any precision or tz-awareness, or plain
    # strings) to whole unix-epoch seconds. Normalizes to UTC-aware first (via
    # pd.to_datetime(..., utc=True) -- real InfluxDB `_time` values are already
    # tz-aware, and naive input is assumed to already be UTC), then explicitly
    # upcasts to nanosecond precision BEFORE dividing by 10**9 -- pandas/the
    # influxdb client don't always hand back the same precision (a pivoted Flux
    # query result parses to datetime64[ns, UTC], but a non-pivoted one has been
    # observed to parse to datetime64[us, UTC] instead), and dividing an
    # already-microsecond int64 by 10**9 silently produces a value 1000x too
    # small with no error. Staying tz-aware throughout the upcast matters: casting
    # straight to a tz-naive dtype (e.g. 'datetime64[ns]') raises on tz-aware
    # input in current pandas rather than silently dropping the offset.
    #
    @staticmethod
    def _time_column_to_unix_epoch_s(time_series : pd.Series) -> pd.Series:
        return pd.to_datetime(time_series, utc = True).astype('datetime64[ns, UTC]').astype('int64') // 10**9

    #
    # Run a Flux query
    #
    def run_flux_query_on_forex_database_and_get_dataframe(self, query : str) -> pd.DataFrame:
        query_api = self.client.query_api()
        df = (
            query_api
            .query_data_frame(query, org = self.INFLUXDB_ORG)
        )

        for column_name in ['result', 'table']:
            if column_name in df.columns:
                df.drop(columns = [column_name], inplace = True)

        if '_time' in df.columns:
            df['unix_epoch_s'] = InfluxDbTool._time_column_to_unix_epoch_s(df['_time'])
            df.drop(columns = ['_time'], inplace = True)
            column_list = ['unix_epoch_s']
            column_list.extend([x for x in df.columns if not x == 'unix_epoch_s'])
            df = df[column_list]

        return df

    #
    # Validate point (for data insertion)
    #
    def validate_point(
            measurement,
            tags,
            fields,
            ALLOWED_TAGS,
            ALLOWED_FIELDS,
            timestamp,
            write_precision_str = 's',
    ):

        # --- schema checks ---
        extra_tags = set(tags) - ALLOWED_TAGS
        if extra_tags:
            raise ValueError(f"Unexpected tag(s): {extra_tags}")

        for k, v in fields.items():
            if k not in ALLOWED_FIELDS:
                raise ValueError(f"Unexpected field: {k}")
            if not isinstance(v, ALLOWED_FIELDS[k]):
                raise TypeError(f"Field {k} must be {ALLOWED_FIELDS[k].__name__}")

        # --- build point ---
        p = Point(measurement)
        for k, v in tags.items():
            p = p.tag(k, v)
        for k, v in fields.items():
            p = p.field(k, v)

        write_precision = WritePrecision.NS
        if write_precision_str == 's':
            write_precision = WritePrecision.S

        p = p.time(timestamp, write_precision)

        return p

    #
    # bulk insert
    #
    def insert_dictionary_list(
        self,
        list_of_dictionaries_to_insert,
        ALLOWED_TAGS,
        ALLOWED_FIELDS,
        INFLUXDB_BUCKET,
        batch_size = 2000,
        write_precision_str = 's',
    ):

        write_api = self.client.write_api(write_options = SYNCHRONOUS)

        points = []
        for item in list_of_dictionaries_to_insert:

            p = InfluxDbTool.validate_point(
                item['measurement'],
                item['tags'],
                item['fields'],
                ALLOWED_TAGS,
                ALLOWED_FIELDS,
                item['time'],
            )
            points.append(p)

        write_precision = WritePrecision.NS
        if write_precision_str == 's':
            write_precision = WritePrecision.S

        for i in range(0, len(points), batch_size):
            batch = points[i:i + batch_size]
            write_api.write(
                bucket = INFLUXDB_BUCKET,
                record = batch,
                write_precision = write_precision,
            )
