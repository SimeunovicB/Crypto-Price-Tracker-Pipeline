import logging
import os

import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

HEALTHCHECK_URL = os.getenv("HEALTHCHECK_URL")


def send_heartbeat():
    if not HEALTHCHECK_URL:
        log.warning("HEALTHCHECK_URL not set — skipping heartbeat")
        return
    try:
        requests.get(HEALTHCHECK_URL, timeout=5)
        log.info("Heartbeat sent successfully.")
    except requests.RequestException as e:
        log.warning(f"Healthcheck failed: {e}")
