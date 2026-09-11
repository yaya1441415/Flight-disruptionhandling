import json
import boto3
import os


events_client = boto3.client("events")
EVENT_BUS_NAME = os.environ["EVENT_BUS_NAME"]

def handler(event, context):
    request = json.loads(event["body"])
    print(request)
    resp = events_client.put_events(
        Entries=[
            {
                "Source": "flight-ops",
                "DetailType": "FlightDisrupted",
                "Detail": json.dumps(request),
                "EventBusName": EVENT_BUS_NAME,
            }
        ]
    )

    if resp["FailedEntryCount"]>0:
        return {"statusCode": 500, "body": "failed to publish event"}

    return {"statusCode": 202, "body": "Accepted"}

