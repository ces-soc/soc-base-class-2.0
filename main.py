from cessoc import humio
from cessoc.rabbitmq import rabbitmq

humio.write(data=[{"test":"test"}], endpoint="https://secops-humio-dev.byu.edu/api/v1/ingest/humio-unstructured", token="57c1a7b7-086c-432b-96f0-960636ece63e")

rabbitmq.hi

print("finished")