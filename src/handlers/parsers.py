from datetime import datetime
from decimal import Decimal

from aws_lambda_powertools import Logger
import requests

from src.aws_resources.dynamodb import save_rate
from src.handlers.utils import generate_id


logger = Logger(service="rates-aggregator", level="DEBUG")

USD = "USD"
EUR = "EUR"
RUB = "RUB"


@logger.inject_lambda_context(log_event=True)
def parse_privatbank(event, _):
    url = "https://api.privatbank.ua/p24api/pubinfo?json&exchange&coursid=5"
    rates = requests.get(url).json()
    print(rates)
    for rate in rates:
        if rate["base_ccy"] == "UAH":
            rate_kwargs = {
                "rate_id": generate_id(),
                "currency":  ccy if (ccy := rate.get("ccy")) != 'RUR' else RUB,
                "buy": Decimal(rate["buy"][:6]),
                "sale": Decimal(rate["sale"][:6]),
                "source": "privatbank",
                "created": round(datetime.timestamp(datetime.now()))
            }
            save_rate(**rate_kwargs)

    return event


@logger.inject_lambda_context(log_event=True)
def parse_monobank(event, _):
    url = 'https://api.monobank.ua/bank/currency'
    response = requests.get(url)
    if not response.ok:
        return

    r_json = response.json()
    if r_json != {'errorDescription': 'Too many requests'}:
        for rate in r_json:
            if (rate['currencyCodeA'] in {840, 978, 643}) and (rate['currencyCodeB'] == 980):
                if rate['currencyCodeA'] == 840:
                    currency = USD
                elif rate['currencyCodeA'] == 978:
                    currency = EUR
                elif rate['currencyCodeA'] == 643:
                    currency = RUB

                rate_kwargs = {
                    "rate_id": generate_id(),
                    "currency": currency,
                    "buy": Decimal(str(round(rate['rateBuy'], 3))),
                    "sale": Decimal(str(round(rate['rateSell'], 3))),
                    "source": "monobank",
                    "created": round(datetime.timestamp(datetime.now()))
                }
                save_rate(**rate_kwargs)


@logger.inject_lambda_context(log_event=True)
def parse_vkurse(event, _):
    url = "http://vkurse.dp.ua/course.json"
    response = requests.get(url)
    if not response.ok:
        return

    r_json = response.json()
    for curr in r_json:
        if curr == 'Dollar':
            currency = USD
        elif curr == 'Euro':
            currency = EUR
        else:
            currency = RUB

        buy = r_json[curr]['buy'].replace(',', '.')
        sale = r_json[curr]['sale'].replace(',', '.')

        rate_kwargs = {
            "rate_id": generate_id(),
            'currency': currency,
            'buy': Decimal(buy),
            'sale': Decimal(sale),
            'source': "vkurse",
            "created": round(datetime.timestamp(datetime.now()))
        }

        save_rate(**rate_kwargs)

