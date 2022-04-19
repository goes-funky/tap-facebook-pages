"""Stream class for tap-facebook-pages."""
import time as t
import datetime
import re
import sys
import copy
import json
from pathlib import Path
from typing import Any, Dict, Optional, Iterable, cast
from singer_sdk.streams import RESTStream
import backoff
import functools

import singer
from singer import metadata

import urllib.parse
import requests
import logging

logger = logging.getLogger("tap-facebook-pages")
logger_handler = logging.StreamHandler(stream=sys.stderr)
logger.addHandler(logger_handler)
logger.setLevel("INFO")
logger_handler.setFormatter(logging.Formatter('%(levelname)s %(message)s'))

NEXT_FACEBOOK_PAGE = "NEXT_FACEBOOK_PAGE"
MAX_RETRY = 5
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

BASE_URL = "https://graph.facebook.com/v10.0/{page_id}"


def is_status_code_fn(blacklist=None, whitelist=None):
    def gen_fn(exc):
        status_code = getattr(exc, 'code', None)
        if status_code is None:
            return False
        status_code = getattr(exc, 'code', None)
        if status_code is None:
            return False

        if blacklist is not None and status_code not in blacklist:
            return True

        if whitelist is not None and status_code in whitelist:
            return True

        # Retry other errors up to the max
        return False

    return gen_fn


def retry_handler(details):
    """
        Customize retrying on Exception by updating until with reduced time
        (until - since) should be 90 days
    """
    # Don't have to wait, just update 'until' param in prepared request
    details["wait"] = 0
    args = details["args"]
    for arg in args:
        # decompose url in parts - get and update until param
        if hasattr(arg, "url"):
            url = args[args.index(arg)].url
            parsed_url = urllib.parse.urlparse(url)
            params = urllib.parse.parse_qs(parsed_url.query)

            since, until = params.get("since", False), params.get("until", False)
            if since and until:
                days = int(int(until[0]) - int(since[0]) / 2)
                new_until = int(since[0]) + days  # ** details["tries"]
                logger.info("Updating time period into %s days", days/86400)  # converted from seconds

                # update timeframe with until
                url = url.replace(params["until"][0], str(new_until))
                args[args.index(arg)].url = url

                # 'https://graph.facebook.com/v10.0/100626925472210/published_posts?since=1641787200&access_token=EAAW79ZCLSyFUBANmm1qg6mwFjsjxibIKfPNZBRT3SNZC5MjD5ptObcg47sasiCAwlLIZAvRiONvUyoAjDCrEy7ots3nvmYCZAXXruU6lutUCF4fOvBs79btnJJEZBk2yZAT5JIi90S56hZAEVH9nt9QuVZBsRi527KD0EZBc6StagIQnYZANwVLf9Q8&limit=100&until=1649476800&fields=id%2Ccreated_time%2Cinsights.metric%28post_engaged_users%2Cpost_negative_feedback%2Cpost_negative_feedback_unique%2Cpost_negative_feedback_by_type%2Cpost_negative_feedback_by_type_unique%2Cpost_engaged_fan%2Cpost_clicks%2Cpost_clicks_unique%2Cpost_clicks_by_type%2Cpost_clicks_by_type_unique%29'
                # 'https://graph.facebook.com/v10.0/100626925472210/published_posts?since=1641787200&access_token=EAAW79ZCLSyFUBAJ0BAFxShZAzZBFTDPCZBwAXOjZCYYTOsC2rkpd3csk774aXtH5ZCZC13GKd4UZBD95cfHC0JOiS0MZBjERmZAnyWkyxIdhSzZAELPPHDGBmTCGKWkKlQzlIyxwo5dmZAZCMSOYhvn0SaCz76Lllp0b25OMtJQRYgwIepGAf1RpVHb0A&limit=100&until=1645632000&fields=id%2Ccreated_time%2Cinsights.metric%28post_engaged_users%2Cpost_negative_feedback%2Cpost_negative_feedback_unique%2Cpost_negative_feedback_by_type%2Cpost_negative_feedback_by_type_unique%2Cpost_engaged_fan%2Cpost_clicks%2Cpost_clicks_unique%2Cpost_clicks_by_type%2Cpost_clicks_by_type_unique%29'

                # Update url for the next call
                details.update({
                    'args': args,
                })

    logger.info("Too many data requested. Retrying with half period -- Retry %s/%s", details['tries'], MAX_RETRY)


def error_handler(fnc):
    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=MAX_RETRY,
        giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
        factor=2,
    )
    @backoff.on_exception(
        backoff.expo,
        TooManyDataRequestedError,
        on_backoff=retry_handler,
        max_tries=MAX_RETRY,
        giveup=is_status_code_fn(blacklist=[500]),
        jitter=None,
        max_value=60
    )
    @functools.wraps(fnc)
    def wrapper(*args, **kwargs):
        return fnc(*args, **kwargs)

    return wrapper


class TooManyDataRequestedError(Exception):
    def __init__(self, msg=None, code=None):
        Exception.__init__(self, msg)
        self.code = code


class FacebookPagesStream(RESTStream):
    access_tokens = {}
    metrics = []
    partitions = []
    page_id: str

    def request_records(self, partition: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.
        """
        self.logger.info("Reading data for {}".format(partition and partition.get("page_id", False)))

        next_page_token: Any = None
        finished = False
        while not finished:
            prepared_request = self.prepare_request(
                partition, next_page_token=next_page_token
            )
            try:
                resp = self._request_with_backoff(prepared_request)
                for row in self.parse_response(resp):
                    yield row
                previous_token = copy.deepcopy(next_page_token)
                next_page_token = self.get_next_page_token(
                    response=resp, previous_token=previous_token
                )
                if next_page_token and next_page_token == previous_token:
                    raise RuntimeError(
                        f"Loop detected in pagination. "
                        f"Pagination token {next_page_token} is identical to prior token."
                    )
                # Cycle until get_next_page_token() no longer returns a value
                finished = not next_page_token
            except Exception as e:
                self.logger.warning(e)
                finished = not next_page_token

    def prepare_request(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> requests.PreparedRequest:
        req = super().prepare_request(partition, next_page_token)
        self.logger.info(re.sub("access_token=[a-zA-Z0-9]+&", "access_token=*****&", urllib.parse.unquote(req.url)))
        return req

    @property
    def url_base(self) -> str:
        return BASE_URL

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        self.page_id = partition["page_id"]
        if next_page_token:
            return urllib.parse.parse_qs(urllib.parse.urlparse(next_page_token).query)

        params = {}

        starting_datetime = self.get_starting_timestamp(partition)
        if starting_datetime:
            start_date_timestamp = int(starting_datetime.timestamp())
            params.update({"since": start_date_timestamp})

        if partition["page_id"] in self.access_tokens:
            params.update({"access_token": self.access_tokens[partition["page_id"]]})
        else:
            self.logger.info("Not enough rights for page: " + partition["page_id"])

        params.update({"limit": 100})
        return params

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any] = None) -> Any:
        resp_json = response.json()
        if "paging" in resp_json and "next" in resp_json["paging"]:
            return resp_json["paging"]["next"]
        return None

    def post_process(self, row: dict, partition: dict) -> dict:
        if "page_id" in partition:
            row["page_id"] = partition["page_id"]
        return row

    @property
    def _singer_metadata(self) -> dict:
        """Return metadata object (dict) as specified in the Singer spec.

        Metadata from an input catalog will override standard metadata.
        """
        if self._tap_input_catalog:
            catalog = singer.Catalog.from_dict(self._tap_input_catalog)
            catalog_entry = catalog.get_stream(self.tap_stream_id)
            if catalog_entry:
                return cast(dict, catalog_entry.metadata)

        # Fix replication method to pass state
        md = cast(
            dict,
            metadata.get_standard_metadata(
                schema=self.schema,
                replication_method=self.replication_method,
                key_properties=self.primary_keys or None,
                valid_replication_keys=(
                    [self.replication_key] if self.replication_key else None
                ),
                schema_name=None,
            ),
        )
        return md

    def get_stream_or_partition_state(self, partition: Optional[dict]) -> dict:
        """Return partition state if applicable; else return stream state."""
        state = self.stream_state
        if partition:
            state = self.get_partition_state(partition)

        if "progress_markers" in state and isinstance(state.get("progress_markers", False), list):
            state["progress_markers"] = {}
        return state

    @error_handler
    def _request_with_backoff(self, prepared_request) -> requests.Response:
        response = self.requests_session.send(prepared_request)
        if response.status_code in [401, 403]:
            self.logger.info("Skipping request to {}".format(prepared_request.url))
            self.logger.info(
                f"Reason: {response.status_code} - {str(response.content)}"
            )
            raise RuntimeError(
                "Requested resource was unauthorized, forbidden, or not found."
            )
        elif response.status_code >= 400:
            # retry by changing 'until' param
            error = json.loads(response.content.decode("utf-8")).get("error", {})
            if error.get("code", False) == 1 and error.get("error_subcode", ) == 99:
                message = error.get("message", False) or "Too many data requested"
                raise TooManyDataRequestedError(message, code=500)

            raise RuntimeError(
                f"Error making request to API: {prepared_request.url} "
                f"[{response.status_code} - {str(response.content)}]".replace(
                    "\\n", "\n"
                )
            )
        logging.debug("Response received successfully.")
        return response


class Page(FacebookPagesStream):
    name = "page"
    tap_stream_id = "page"
    path = ""
    primary_keys = ["id"]
    replication_key = None
    forced_replication_method = "FULL_TABLE"
    schema_filepath = SCHEMAS_DIR / "page.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        params = super().get_url_params(partition, next_page_token)
        fields = ','.join(self.config['columns']) if 'columns' in self.config else ','.join(
            self.schema["properties"].keys())
        params.update({"fields": fields})
        return params

    def post_process(self, row: dict, stream_or_partition_state: dict) -> dict:
        return row


class Posts(FacebookPagesStream):
    name = "posts"
    tap_stream_id = "posts"
    path = "/posts"
    primary_keys = ["id"]
    replication_key = "created_time"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "posts.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:

        params = super().get_url_params(partition, next_page_token)
        if next_page_token:
            return params

        fields = ','.join(self.config['columns']) if 'columns' in self.config else ','.join(
            self.schema["properties"].keys())
        params.update({"fields": fields})
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            row["page_id"] = self.page_id
            yield row


class PostTaggedProfile(FacebookPagesStream):
    name = "post_tagged_profile"
    tap_stream_id = "post_tagged_profile"
    path = "/posts"
    primary_keys = ["id"]
    replication_key = "post_created_time"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "post_tagged_profile.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        params = super().get_url_params(partition, next_page_token)
        if next_page_token:
            return params

        params.update({"fields": "id,created_time,to"})
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            parent_info = {
                "page_id": self.page_id,
                "post_id": row["id"],
                "post_created_time": row["created_time"]
            }
            if "to" in row:
                for attachment in row["to"]["data"]:
                    attachment.update(parent_info)
                    yield attachment


class PostAttachments(FacebookPagesStream):
    name = "post_attachments"
    tap_stream_id = "post_attachments"
    path = "/posts"
    primary_keys = ["id"]
    replication_key = "post_created_time"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "post_attachments.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        params = super().get_url_params(partition, next_page_token)
        if next_page_token:
            return params

        params.update({"fields": "id,created_time,attachments"})
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            parent_info = {
                "page_id": self.page_id,
                "post_id": row["id"],
                "post_created_time": row["created_time"]
            }
            if "attachments" in row:
                for attachment in row["attachments"]["data"]:
                    if "subattachments" in attachment:
                        for sub_attachment in attachment["subattachments"]["data"]:
                            sub_attachment.update(parent_info)
                            yield sub_attachment
                        attachment.pop("subattachments")
                    attachment.update(parent_info)
                    yield attachment


class PageInsights(FacebookPagesStream):
    name = None
    tap_stream_id = None
    path = "/insights"
    primary_keys = ["id"]
    replication_key = None
    forced_replication_method = "FULL_TABLE"
    schema_filepath = SCHEMAS_DIR / "page_insights.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        params = super().get_url_params(partition, next_page_token)
        time = int(t.time())
        day = int(datetime.timedelta(1).total_seconds())
        if not next_page_token:
            until = params['since'] + 8035200
            params.update({"until": until if until <= time else time-day})
        else:
            until = params['until'][0]
            if int(until) > time:
                params['until'][0] = str(time-day)
        params.update({"metric": ",".join(self.metrics)})
        return params

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any] = None) -> Any:
        resp_json = response.json()
        if "paging" in resp_json and "next" in resp_json["paging"]:
            time = int(t.time())
            day = int(datetime.timedelta(2).total_seconds())
            params = urllib.parse.parse_qs(urllib.parse.urlparse(resp_json["paging"]["next"]).query)
            since = int(params['since'][0])
            until = int(params['until'][0])
            if since >= time-day or (until >= time and until <= time + day ):
                return None
            return resp_json["paging"]["next"]
        return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            base_item = {
                "name": row["name"],
                "period": row["period"],
                "title": row["title"],
                "id": row["id"],
            }
            if "values" in row:
                for values in row["values"]:
                    if isinstance(values["value"], dict):
                        for key, value in values["value"].items():
                            item = {
                                "context": key,
                                "value": value,
                                "end_time": values["end_time"]
                            }
                            item.update(base_item)
                            yield item
                    else:
                        values.update(base_item)
                        yield values


class PostInsights(FacebookPagesStream):
    name = ""
    tap_stream_id = ""
    # use published_posts instead of feed, as the last one is problematic endpoint
    # path = "/feed"
    path = "/published_posts"
    primary_keys = ["id"]
    replication_key = "post_created_time"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "post_insights.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        params = super().get_url_params(partition, next_page_token)
        time = int(t.time())
        day = int(datetime.timedelta(1).total_seconds())
        if not next_page_token:
            until = params['since'] + 7689600  # 8035200
            params.update({"until": until if until <= time else time-day})
        else:
            until = params['until'][0]
            if int(until) > time:
                params['until'][0] = str(time-day)

        params.update({"fields": "id,created_time,insights.metric(" + ",".join(self.metrics) + ")"})
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            for insights in row["insights"]["data"]:
                base_item = {
                    "post_id": row["id"],
                    "page_id": self.page_id,
                    "post_created_time": row["created_time"],
                    "name": insights["name"],
                    "period": insights["period"],
                    "title": insights["title"],
                    "description": insights["description"],
                    "id": insights["id"],
                }
                if "values" in insights:
                    for values in insights["values"]:
                        if isinstance(values["value"], dict):
                            for key, value in values["value"].items():
                                item = {
                                    "context": key,
                                    "value": value,
                                }
                                item.update(base_item)
                                yield item
                        else:
                            values.update(base_item)
                            yield values
