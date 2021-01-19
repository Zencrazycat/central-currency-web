import os

import boto3
from boto3.dynamodb.conditions import Key


dynamodb = boto3.resource("dynamodb")
client = boto3.client("dynamodb")
currency_rates_table = dynamodb.Table(os.getenv("CURRENCY_RATES_TABLE_NAME"))

TYPE_SOURCE = "SOURCE"
TYPE_RATE = "RATE"
TABLE_GSI = "gsi1"


def generate_key(type, name):
    return f"{type}#{name}"


def save_rate(rate_id, currency, buy, sale, source, created):
    pk = generate_key(TYPE_SOURCE, source)
    sk = generate_key(TYPE_RATE, rate_id)
    data = {
        "pk": pk,
        "sk": sk,
        "type": TYPE_RATE,
        "currency": currency,
        "buy": buy,
        "sale": sale,
        "created": created
    }
    response = currency_rates_table.put_item(Item=data)
    return response


def get_rates():
    response = currency_rates_table.query(IndexName=TABLE_GSI, KeyConditionExpression=Key("type").eq(TYPE_RATE))
    items = response["Items"]
    return items


def get_sources():
    response = currency_rates_table.query(IndexName=TABLE_GSI, KeyConditionExpression=Key("type").eq(TYPE_SOURCE))
    items = response["Items"]
    return items
