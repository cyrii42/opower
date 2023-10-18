import os

from dotenv import load_dotenv

load_dotenv()

CONED_UTILITY_NAME = "coned"
CONED_USERNAME = os.getenv("CONED_USERNAME")
CONED_PASSWORD = os.getenv("CONED_PASSWORD")
CONED_MFA_SECRET = os.getenv("CONED_MFA_SECRET")
