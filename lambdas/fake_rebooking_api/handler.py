import os
import random

def handler(event, context):
    FAILURE_RATE = float(os.environ["FAILURE_RATE"])

    number = random.random()

    if FAILURE_RATE>number:
        return {"statusCode": 500, "body": "event has not succede"}


    return {"statusCode": 200, "body": "event has succede"}