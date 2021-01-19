from datetime import datetime
from decimal import Decimal

import requests

from src.aws_resources.dynamodb import save_rate
from src.handlers.utils import generate_id


def parse_privat(event, context):
    url = "https://api.privatbank.ua/p24api/pubinfo?json&exchange&coursid=5"
    rates = requests.get(url).json()
    print(rates)
    for rate in rates:
        if rate["base_ccy"] == "UAH":
            rate_kwargs = {
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
