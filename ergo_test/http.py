import logging

import requests
from requests.adapters import HTTPAdapter, Retry

from ergo_test import FunctionComponent


class HTTPComponent(FunctionComponent):
    @property
    def namespace(self):
        return {
            "protocol": "http",
        }


def http_session() -> requests.Session:
    # requests need to retry on ConnectionError while our HTTP server boots.
    session = requests.Session()
    retries = Retry(total=None, connect=50, backoff_factor=0.00001)
    session.mount("http://", HTTPAdapter(max_retries=retries))
    # these retries generate a ton of warnings
    logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
    return session
