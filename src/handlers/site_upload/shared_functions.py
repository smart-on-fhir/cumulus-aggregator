""" Functions used across different lambdas"""
import json


def http_response(status: int, body: str):
    """Generates the payload AWS lambda expects as a return value"""
    return {
        "isBase64Encoded": False,
        "statusCode": status,
        "body": json.dumps(body),
        "headers": {"Content-Type": "application/json"},
    }
