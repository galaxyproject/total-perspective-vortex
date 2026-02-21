import os
from typing import Any
from urllib.parse import urlparse

import requests
from ruamel.yaml import YAML


def load_yaml_from_url_or_path(url_or_path: str) -> Any:
    yaml = YAML(typ="rt")
    parsed = urlparse(url_or_path)
    is_http_url = parsed.scheme in {"http", "https"}
    if not is_http_url:
        with open(url_or_path, "r") as f:
            return yaml.load(f)
    else:
        with requests.get(url_or_path) as r:
            return yaml.load(r.content)
