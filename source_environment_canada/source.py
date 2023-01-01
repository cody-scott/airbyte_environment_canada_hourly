#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

import datetime
"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


# Basic full refresh stream
class EnvironmentCanada(HttpStream):
    date_field_name = "date"

    url_base = "https://api.weather.gc.ca/"
    primary_key = None

    def __init__(self, climate_id: str, start_date: str, **kwargs):
        super().__init__(**kwargs)
        self.climate_id = climate_id
        self.start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")

    def path(self, **kwargs):
        return 'collections/climate-hourly/items'

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        current_date = stream_slice[self.date_field_name]
        return {
            'CLIMATE_IDENTIFIER': self.climate_id,
            "LOCAL_YEAR": current_date.year,
            "LOCAL_MONTH": current_date.month,
            "LOCAL_DAY": current_date.day,
            "limit": 24
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        response = response.json()['features']
        return response

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        stream_state = stream_state or {}
        start_date = self.start_date
        return chunk_date_range(start_date)

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]):
        current_stream_state = current_stream_state or {}
        current_stream_state[self.date_field_name] = max(
            latest_record[self.date_field_name], current_stream_state.get(self.date_field_name, self._start_date)
        )
        return current_stream_state

def chunk_date_range(start_date: datetime.datetime) -> Iterable[Mapping[str, Any]]:
    """ 
    Returns a list of each day between the start date and now. Ignore weekends since exchanges don't run on weekends.
    The return value is a list of dicts {'date': date_string}.
    """
    days = []
    now = datetime.datetime.now()
    while start_date < now:
        days.append({"date": start_date})
        start_date += datetime.timedelta(days=1)

    return days


# Source
class SourceEnvironmentCanada(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.
        # auth = TokenAuthenticator(token="api_key")  # Oauth2Authenticator is also available if you need oauth support
        # return [Customers(authenticator=auth), Employees(authenticator=auth)]
        return [EnvironmentCanada(
            climate_id=config['climate_id'],
            start_date=config['start_date']
        )]
