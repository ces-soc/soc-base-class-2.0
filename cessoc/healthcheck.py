"""
The healthcheck package provides standard healthcheck functionality for cessoc services.
"""
import time
from datetime import datetime, timedelta
import tzlocal
import os
from typing import Optional, Union
from cessoc import humio
from cessoc.aws import ssm


class HealthCheck:
    """
    Sends information to humio with pre-defined fields in addition to a custom field.
    """
    def __init__(self, service_name: str):
        """
        Initializes the healthcheck object.

        :param service_name: The name of the service sending the healthcheck
        """

        self.start_time = time.time()
        self.service_name = service_name
        self.campus = os.environ["CAMPUS"].lower()
        self.timezone = tzlocal.get_localzone()

    def send(self, custom_data: Union[str, dict] = "None", endpoint: Optional[str] = None, token: Optional[str] = None):
        """
        Sends the healthcheck data to humio.

        :param custom_data: The custom data to be sent to humio. Must be a json object
        :param token: The humio ingest token
        :param endpoint: The humio ingest endpoint
        """
        if endpoint is None:
            endpoint = ssm.get_value(f"/{self.campus}/secops-humio/config/api_endpoint") + "ingest/humio-unstructured"
        if token is None:
            token = ssm.get_value(f"/{self.campus}/secops-humio/secrets/healthcheck/ingest_token")
        # set end time
        self.end_time = time.time()
        # Make the runtime easily readable
        readable_runtime = timedelta(seconds = round(self.end_time - self.start_time)).__str__().split(":")
        readable_runtime = f"{readable_runtime[0]}h {readable_runtime[1]}m {readable_runtime[2]}s"
        # data to send to humio healthcheck
        healthdata = [{
            "start_time": f"{datetime.fromtimestamp(self.start_time, tz=self.timezone)}",
            "end_time": f"{datetime.fromtimestamp(self.end_time, tz=self.timezone)}", 
            "service_name": f"{self.service_name}",
            "data": custom_data,
            "runtime": f"{readable_runtime}",
            "campus": f"{self.campus}",
            "@timestamp": f"{self.end_time}"
        }]

        humio.write(data=healthdata, endpoint=endpoint, token=token, path="healthcheck")
