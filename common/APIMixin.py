from urllib.error import HTTPError
from urllib.parse import urlencode

import requests
import logging


class APIMixin:
    def __init__(self, is_proxy):
        self.is_proxy_url = is_proxy
    def get_request(self, url, headers, payload,verify=True):
        # print("url to hit =>",url)
        if self.is_proxy_url:
            url = self.get_proxy_url(url)
        else:
            url = url

        try:
            response = requests.get(url, headers=headers, data=payload,verify=verify)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response
        except requests.exceptions.RequestException as e:
            logging.error('Unable to fetch %s: %s', url, e)
            return None
        except HTTPError as he:
            logging.error('Unable to fetch %s: %s', url, he)
            return None

    def post_request(self, url, headers, json, data=None):

        try:
            if data is None:
                response = requests.post(
                    url,
                    headers=headers,
                    json=json,
                    timeout=60
                )
            else:
                response = requests.post(
                    url,
                    headers=headers,
                    data=data,
                    timeout=60
                )

            response.raise_for_status()  # Raise an exception for HTTP errors
            return response
        except requests.exceptions.RequestException as e:
            logging.error('Unable to fetch %s: %s', url, e)
            return None
        except HTTPError as he:
            logging.error('Unable to fetch %s: %s', url, he)
            return None

    @staticmethod
    def get_proxy_url(url):
        payload = {'api_key': "f6727157-41b3-4edb-b3a9-7c4b8b2837c2", 'url': url}
        proxy_url = 'https://proxy.scrapeops.io/v1/?' + urlencode(payload)
        return proxy_url
