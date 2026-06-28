#
# load useful libraries
#
import json

# figure out how to use this path relatively
from python_tools_and_shortcuts.aws.secrets_manager import get_secret

#
# User settings. Whether this is best hardcoded or otherwise remains to be determined
#
secret_name = 'Forex/InfluxDbPassword'

#
# retrieve the protected information
#
secret_str = get_secret(secret_name)
secret = json.loads(secret_str)
INFLUXDB_URL = secret['INFLUXDB_URL']
INFLUXDB_TOKEN = secret['INFLUXDB_TOKEN']
INFLUXDB_ORG = secret['INFLUXDB_ORG']
INFLUXDB_BUCKET = secret['INFLUXDB_BUCKET']
