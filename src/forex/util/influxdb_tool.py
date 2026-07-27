import pandas as pd
from influxdb_client import InfluxDBClient, Point, PostBucketRequest, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException


class InfluxDbTool:
    def __init__(
        self,
        url: str,
        token: str,
        org: str,
        timeout: int = 120000,
    ) -> None:
        self.url = url
        self.token = token
        self.org = org
        self.client = InfluxDBClient(url=url, token=token, org=org, timeout=timeout)

    def __del__(self) -> None:
        self.client.close()

    def create_bucket(self, bucket_name: str) -> None:
        buckets_api = self.client.buckets_api()
        org = self.client.organizations_api().find_organizations(org=self.org)[0]
        org_id = org.id

        try:
            existing_bucket = buckets_api.find_bucket_by_name(bucket_name)
            if existing_bucket:
                print(f"Bucket '{bucket_name}' already exists.")
            else:
                bucket_req = PostBucketRequest(org_id=org_id, name=bucket_name)
                bucket = buckets_api.create_bucket(bucket=bucket_req)
                print('Created bucket:', bucket.name, 'schema_type:', bucket.schema_type)
        except ApiException as e:
            print(f'Error creating bucket: {e}')

    def delete_bucket(self, bucket_name: str) -> None:
        buckets_api = self.client.buckets_api()
        buckets = buckets_api.find_buckets().buckets

        try:
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
            print(f'Error deleting bucket: {e}')

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
    def _time_column_to_unix_epoch_s(time_series: pd.Series) -> pd.Series:
        return pd.to_datetime(time_series, utc=True).astype('datetime64[ns, UTC]').astype('int64') // 10**9

    def run_flux_query_on_forex_database_and_get_dataframe(self, query: str) -> pd.DataFrame:
        query_api = self.client.query_api()
        df = query_api.query_data_frame(query, org=self.org)

        for column_name in ['result', 'table']:
            if column_name in df.columns:
                df.drop(columns=[column_name], inplace=True)

        if '_time' in df.columns:
            df['unix_epoch_s'] = InfluxDbTool._time_column_to_unix_epoch_s(df['_time'])
            df.drop(columns=['_time'], inplace=True)
            column_list = ['unix_epoch_s']
            column_list.extend([x for x in df.columns if x != 'unix_epoch_s'])
            df = df[column_list]

        return df

    @staticmethod
    def validate_point(
        measurement: str,
        tags: dict,
        fields: dict,
        allowed_tags: frozenset[str],
        allowed_fields: dict[str, type],
        timestamp: int,
        write_precision_str: str = 's',
    ) -> Point:
        extra_tags = set(tags) - allowed_tags
        if extra_tags:
            raise ValueError(f'Unexpected tag(s): {extra_tags}')

        for k, v in fields.items():
            if k not in allowed_fields:
                raise ValueError(f'Unexpected field: {k}')
            if not isinstance(v, allowed_fields[k]):
                raise TypeError(f'Field {k} must be {allowed_fields[k].__name__}')

        p = Point(measurement)
        for k, v in tags.items():
            p = p.tag(k, v)
        for k, v in fields.items():
            p = p.field(k, v)

        write_precision = WritePrecision.S if write_precision_str == 's' else WritePrecision.NS
        return p.time(timestamp, write_precision)

    def insert_dictionary_list(
        self,
        list_of_dictionaries_to_insert: list[dict],
        allowed_tags: frozenset[str],
        allowed_fields: dict[str, type],
        bucket: str,
        batch_size: int = 2000,
        write_precision_str: str = 's',
    ) -> None:
        write_api = self.client.write_api(write_options=SYNCHRONOUS)

        points = [
            InfluxDbTool.validate_point(
                item['measurement'],
                item['tags'],
                item['fields'],
                allowed_tags,
                allowed_fields,
                item['time'],
                write_precision_str,
            )
            for item in list_of_dictionaries_to_insert
        ]

        write_precision = WritePrecision.S if write_precision_str == 's' else WritePrecision.NS
        for i in range(0, len(points), batch_size):
            batch = points[i : i + batch_size]
            # influxdb_client's own stubs are inconsistent here: WritePrecision.S/.NS are
            # plain `str` constants at runtime, but write()'s signature wants a stricter
            # Literal type -- the value itself is correct, this is a library typing gap.
            write_api.write(bucket=bucket, record=batch, write_precision=write_precision)  # type: ignore[arg-type]
