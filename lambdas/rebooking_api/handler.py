from botocore.exceptions import ClientError
import os
import boto3
import time
import urllib.request
import urllib.error

table = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])

COOLDOWN_SECONDS = int(os.environ["COOLDOWN_SECONDS"])
FAILURE_THRESHOLD = int(os.environ["FAILURE_THRESHOLD"])
FAKE_API_URL = os.environ["FAKE_API_URL"]

def handler(event, context):
    resp = table.get_item(Key={"pk": "breaker#fake-rebooking-api"})
    item = resp.get("Item")

    state = "CLOSED" if item is None else item['state']

    if state == "OPEN":
        elapsed = time.time()-item["opened_at"]>= COOLDOWN_SECONDS

        if not elapsed:
            return {"statusCode":503, "body": "elpased timenot done yet"}
        else:
            try:
                table.update_item(
                    Key={"pk": "breaker#fake-rebooking-api"},
                    UpdateExpression="SET #s = :half",
                    ConditionExpression="#s = :open",
                    ExpressionAttributeNames={"#s": "state"},
                    ExpressionAttributeValues={":half": "HALF_OPEN", ":open": "OPEN"},
                )
                won_the_claim = True

            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    won_the_claim = False
                else:
                    raise
            if won_the_claim:
                return attempt_call(True)
            return {"statusCode":503, "body": "circuit half open"}
    elif state == "HALF_OPEN":
        return {"statusCode": 503, "body": "circuit open"}
    else:
        return attempt_call(is_probing=False)

def attempt_call(is_probing):
    try:
        with urllib.request.urlopen(FAKE_API_URL, timeout=3) as resp:
            resp.read()
        table.update_item(
            Key={"pk": "breaker#fake-rebooking-api"},
            UpdateExpression="SET #s = :closed, failure_count = :zero",
            ExpressionAttributeNames={"#s": "state"},
            ExpressionAttributeValues={":closed": "CLOSED", ":zero": 0},
        )
        return {"statusCode": 200, "body": "rebooking succeeded"}
    except (urllib.error.URLError, TimeoutError):
        if is_probing:
            table.update_item(
                Key={"pk": "breaker#fake-rebooking-api"},
                UpdateExpression="SET #s = :open, opened_at = :now",
                ExpressionAttributeNames={"#s": "state"},
                ExpressionAttributeValues={":open": "OPEN", ":now": int(time.time())},
            )
        else:
            resp = table.update_item(
                Key={"pk": "breaker#fake-rebooking-api"},
                UpdateExpression="ADD failure_count :incr",
                ExpressionAttributeValues={":incr": 1},
                ReturnValues="UPDATED_NEW",
            )
            if resp["Attributes"]["failure_count"] >= FAILURE_THRESHOLD:
                table.update_item(
                    Key={"pk": "breaker#fake-rebooking-api"},
                    UpdateExpression="SET #s = :open, opened_at = :now",
                    ConditionExpression="attribute_not_exists(#s) OR #s <> :open",
                    ExpressionAttributeNames={"#s": "state"},
                    ExpressionAttributeValues={":open": "OPEN", ":now": int(time.time())},
                )
        return {"statusCode":503, "body": "rebooking failed"}
