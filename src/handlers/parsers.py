from datetime import datetime
from decimal import Decimal
from os import getenv

import requests

from src.aws_resources.dynamodb import save_rate, get_rate
from src.handlers.utils import generate_id


CURRENCY_RATES_TABLE = getenv("CURRENCY_RATES_TABLE")


def parse_privat(event, context):
    url = "https://api.privatbank.ua/p24api/pubinfo?json&exchange&coursid=5"
    rates = requests.get(url).json()
    print(rates)
    for rate in rates:
        if rate["base_ccy"] == "UAH":
            rate_kwargs = {
                "table_name": CURRENCY_RATES_TABLE,
                "rate_id": generate_id(),
                "currency": rate.get("ccy"),
                "buy": Decimal(rate["buy"][:6]),
                "sale": Decimal(rate["sale"][:6]),
                "source": "privat",
                "created": round(datetime.timestamp(datetime.now()))
            }
            save_rate(**rate_kwargs)

    return event


def parse_mono(event, context):
    url = "https://api.monobank.ua/bank/currency"
    response = requests.get(url).json()
    print(response)

    return event


def parse_vkurse(event, context):
    url = "http://vkurse.dp.ua/course.json"
    response = requests.get(url).json()
    print(response)

    return event
