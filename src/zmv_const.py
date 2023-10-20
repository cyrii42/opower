import os
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()

EASTERN_TIME = ZoneInfo("America/New_York")

INFLUX_URL = "http://mac-mini.box:8086"
INFLUX_ORG = "ZMV"
INFLUX_TOKEN_CONED = os.getenv("INFLUX_TOKEN_CONED") # Con Edison token

CONED_UTILITY_NAME = "coned"
CONED_USERNAME = os.getenv("CONED_USERNAME")
CONED_PASSWORD = os.getenv("CONED_PASSWORD")
CONED_MFA_SECRET = os.getenv("CONED_MFA_SECRET")
