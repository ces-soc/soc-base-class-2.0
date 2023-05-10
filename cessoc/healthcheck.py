from cessoc import humio
import time

print("healthcheck imported")

class healthcheck:
    """
    Sends information to humio with pre-defined fields in addition to a custom field.
    """
    def __init__(self):
        self.start_time = time.time()

    def send(self, custom_data, token, service_name):
        """
        Sends the healthcheck data to humio.

        :param custom_data: The custom data to be sent to humio. Must be a json object
        """
        healthdata = f"""
        {
            "start_time": "{self.start_time}",
            "end_time": "{time.time()}",
            "service_name": "{service_name}",
            "data": {custom_data},
        }
        """
        humio.write(data=healthdata, endpoint="endpoint", token=token, path="healthcheck")