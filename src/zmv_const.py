import os
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()

EASTERN_TIME = ZoneInfo("America/New_York")

# InfluxDB setup
INFLUX_URL = os.getenv("INFLUX_URL", "") 
INFLUX_ORG = os.getenv("INFLUX_ORG", "")
INFLUX_TOKEN_HASS = os.getenv("INFLUX_TOKEN_HASS", "") # Home Assistant read-only
INFLUX_TOKEN_CONED = os.getenv("INFLUX_TOKEN_CONED", "") # Con Edison token

CONED_UTILITY_NAME = "coned"
CONED_USERNAME = os.getenv("CONED_USERNAME", "")
CONED_PASSWORD = os.getenv("CONED_PASSWORD", "")
CONED_MFA_SECRET = os.getenv("CONED_MFA_SECRET", "")

CONED_SPREADSHEET = os.getenv("CONED_SPREADSHEET_COPY", "")
