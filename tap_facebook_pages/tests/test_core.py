"""Tests init and discovery features for tap-facebook-pages."""

from singer_sdk.helpers.util import utc_now

from tap_facebook_pages.tap import TapFacebookPages

SAMPLE_CONFIG = {
    "start_date": utc_now()
    # TODO: Initialize minimal tap config and/or register env vars in test harness
}

# TODO: Expand tests as appropriate for your tap.


def test_catalog_discovery():
    """Test stream catalog discovery."""
    tap = TapFacebookPages(
        config=SAMPLE_CONFIG, state=None, parse_env_config=True
    )
    catalog_json = tap.run_discovery()
    assert catalog_json
