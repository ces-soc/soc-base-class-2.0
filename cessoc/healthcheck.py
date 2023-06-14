from cessoc import humio
from cessoc.aws import ssm
import time
from typing import Optional


class healthcheck:
    """
    Sends information to humio with pre-defined fields in addition to a custom field.
    """
    def __init__(self):
        self.start_time = time.time()

    def send(self, custom_data, token, service_name, endpoint: Optional[str] = None):
        """
        Sends the healthcheck data to humio.

        :param custom_data: The custom data to be sent to humio. Must be a json object
        """
        if endpoint is None:
            endpoint = ssm.get_value("/byu/secops-humio/config/api_endpoint") + "ingest/humio-unstructured"
        healthdata = [{
            "start_time": f"{self.start_time}",
            "end_time": f"{time.time()}",
            "service_name": f"{service_name}",
            "data": custom_data,
        }]

        humio.write(data=healthdata, endpoint=endpoint, token=token, path="healthcheck")
