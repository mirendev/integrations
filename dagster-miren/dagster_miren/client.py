import os
import time
from urllib.parse import quote

import requests


class MirenClient:
    def __init__(
        self, endpoint, token_env=None, ca_cert=None, client_cert=None, client_key=None
    ):
        if not endpoint.startswith("https://"):
            raise ValueError("Miren endpoint must use HTTPS")
        if bool(client_cert) != bool(client_key):
            raise ValueError("client_cert and client_key must be supplied together")
        self.endpoint = endpoint.rstrip("/") + "/api/v1"
        self.token_env = token_env
        self.verify = ca_cert or True
        self.cert = (client_cert, client_key) if client_cert else None

    def request(self, method, path, payload=None):
        # Read on each call to allow credential rotation. Never serialize the
        # credential into run tags or Dagster's instance reference.
        headers = {}
        if self.token_env:
            headers["Authorization"] = "Bearer " + os.environ[self.token_env]
        for attempt in range(3):
            try:
                response = requests.request(
                    method,
                    self.endpoint + path,
                    json=payload,
                    headers=headers,
                    verify=self.verify,
                    cert=self.cert,
                    timeout=30,
                )
                response.raise_for_status()
                return response.json()
            except (requests.ConnectionError, requests.Timeout):
                if attempt == 2:
                    raise
                time.sleep(0.5 * (attempt + 1))

    def submit(self, payload):
        return self.request(
            "POST", f"/apps/{quote(payload['app'], safe='')}/runs/submit", payload
        )["id"]

    def get(self, run_id):
        return self.request("GET", f"/runs/{quote(run_id, safe='')}")["run"]

    def cancel(self, run_id):
        return self.request("POST", f"/runs/{quote(run_id, safe='')}/cancel")[
            "canceled"
        ]
