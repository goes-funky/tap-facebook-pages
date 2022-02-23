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

FACEBOOK_API_VERSION = "v10.0"
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
        self.access_tokens = {}
        # update partitions and page (id, token) on sync
        if self.input_catalog:
            self.get_pages_tokens(self.config['page_ids'], self.config['access_token'])
            # for page_id in self.config['page_ids']:
            #     self.access_tokens[page_id] = self.exchange_token(page_id, self.config['access_token'])

        self.partitions = [{"page_id": x} for x in self.config["page_ids"]]

    def get_pages_tokens(self, page_ids: list, access_token: str):
        params = {
            "access_token": access_token,
        }
        response = session.get(ME_URL, params=params)
        response_json = response.json()

        if response.status_code != 200:
            raise Exception(response_json["error"]["message"])

        # Request a list of pages with associated tokens
        params["fields"] = "name,access_token"
        user_id = response_json["id"]
        next_page_cursor = True
        while next_page_cursor:
            response = session.get(ACCOUNTS_URL.format(version=FACEBOOK_API_VERSION, user_id=user_id), params=params)
            response_json = response.json()
            if response.status_code != 200:
                raise Exception(response_json["error"]["message"])

            next_page_cursor = response_json.get("paging", {}).get("cursors", {}).get("after", False)
            params["after"] = next_page_cursor
            for pages in response_json["data"]:
                page_id = pages["id"]
                if page_id in page_ids:
                    self.access_tokens[page_id] = pages["access_token"]

    def discover_streams(self) -> List[Stream]:
        streams = []
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
            catalog = singer.catalog.Catalog.from_dict(self.input_catalog)
            for stream in catalog.streams:

                if stream.is_selected:
                    selected_streams.append(stream.tap_stream_id)

            stream_objects = [x for x in stream_objects if x.tap_stream_id in selected_streams]
            for obj in stream_objects:
                self.logger.info("Found stream: " + obj.tap_stream_id)
        return stream_objects


# CLI Execution:

cli = TapFacebookPages.cli
