"""facebook-pages tap class."""
import json
import logging
from pathlib import PurePath
from typing import List, Union
import requests
import singer
from singer_sdk import Tap, Stream
from singer_sdk.typing import (
    ArrayType,
    DateTimeType,
    PropertiesList,
    Property,
    StringType,
)

from tap_facebook_pages.insights import INSIGHT_STREAMS
from tap_facebook_pages.streams import (
    Page, Posts, PostAttachments, PostTaggedProfile
)

PLUGIN_NAME = "tap-facebook-pages"

STREAM_TYPES = [
    Page,
    Posts,
    PostAttachments,
    PostTaggedProfile,
]

FACEBOOK_API_VERSION = "v18.0"
ACCOUNTS_URL = "https://graph.facebook.com/{version}/{user_id}/accounts"
ME_URL = "https://graph.facebook.com/{version}/me".format(version=FACEBOOK_API_VERSION)
BASE_URL = "https://graph.facebook.com/{page_id}"

session = requests.Session()


class TapFacebookPages(Tap):
    name = PLUGIN_NAME

    _logger = singer.get_logger("FacebookPages")

    config_jsonschema = PropertiesList(
        Property("access_token", StringType, required=True),
        Property("page_ids", ArrayType(StringType), required=True),
        Property("start_date", DateTimeType, required=True),
    ).to_dict()

    def __init__(self, config: Union[PurePath, str, dict, None] = None,
                 catalog: Union[PurePath, str, dict, None] = None, state: Union[PurePath, str, dict, None] = None,
                 parse_env_config: bool = True) -> None:
        super().__init__(config, catalog, state, parse_env_config)
        # update page access tokens on sync
        page_ids = self.config['page_ids']

    def exchange_token(self, page_id: str, access_token: str):
        url = BASE_URL.format(page_id=page_id)
        data = {
            'fields': 'access_token,name',
            'access_token': access_token
        }

        self.logger.info("Exchanging access token for page with id=" + page_id)
        response = session.get(url=url, params=data)
        response_data = json.loads(response.text)
        if response.status_code != 200:
            if response.status_code in (4, 17, 32, 613):
                self.logger.info("Retry to re-run after 1 hour ...")
            error_message = "Failed exchanging token: " + response_data["error"]["message"]
            self.logger.error(error_message)
            raise RuntimeError(
                error_message
            )
        self.logger.info("Successfully exchanged access token for page with id=" + page_id)
        return response_data['access_token']

    def get_pages_tokens(self, page_ids: list, access_token: str):
        params = {
            "access_token": access_token,
        }
        response = session.get(ME_URL, params=params)
        response_json = response.json()

        if response.status_code != 200:
            raise Exception(response_json["error"]["message"])

        # Get Access Tokens of Pages the user Manages
        params["fields"] = "name,access_token"
        params.update({"limit": 100})

        user_id = response_json["id"]
        next_page_cursor = True
        while next_page_cursor:
            response = session.get(ACCOUNTS_URL.format(version=FACEBOOK_API_VERSION, user_id=user_id), params=params)
            response_json = response.json()
            if response.status_code != 200:
                if response.status_code in (4, 17, 32, 613):
                    self.logger.info("Retry to re-run after 1 hour ...")
                raise Exception(response_json["error"]["message"])

            next_page_cursor = response_json.get("paging", {}).get("cursors", {}).get("after", False)
            params["after"] = next_page_cursor
            for pages in response_json["data"]:
                page_id = pages["id"]
                if page_id not in page_ids:
                    continue

                self.logger.info("Get token for page '{}'".format(pages["name"]))
                self.access_tokens[page_id] = pages["access_token"]

    def discover_streams(self) -> List[Stream]:
        streams = []
        # update page access tokens on sync
        page_ids = self.config['page_ids']
        self.access_tokens = {}
        self.partitions = [{"page_id": x} for x in page_ids]
        if self.input_catalog:
            if len(page_ids) > 1:
                self.get_pages_tokens(page_ids, self.config['access_token'])
            else:
                self.access_tokens[page_ids[0]] = self.exchange_token(page_ids[0], self.config['access_token'])
        for stream_class in STREAM_TYPES:
            stream = stream_class(tap=self)
            stream.partitions = self.partitions
            stream.access_tokens = self.access_tokens
            streams.append(stream)

        for insight_stream in INSIGHT_STREAMS:
            stream = insight_stream["class"](tap=self, name=insight_stream["name"])
            stream.tap_stream_id = insight_stream["name"]
            stream.metrics = insight_stream["metrics"]
            stream.partitions = self.partitions
            stream.access_tokens = self.access_tokens
            streams.append(stream)
        return streams

    def load_streams(self) -> List[Stream]:
        stream_objects = self.discover_streams()
        if self.input_catalog:
            selected_streams = []
            catalog = self.input_catalog
            for stream in catalog.streams:

                if stream.is_selected:
                    selected_streams.append(stream.tap_stream_id)

            stream_objects = [x for x in stream_objects if x.tap_stream_id in selected_streams]
            for obj in stream_objects:
                self.logger.info("Found stream: " + obj.tap_stream_id)
        return stream_objects


# CLI Execution:

cli = TapFacebookPages.cli
