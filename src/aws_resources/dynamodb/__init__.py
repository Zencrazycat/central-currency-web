import boto3

dynamodb = boto3.resource("dynamodb")
client = boto3.client("dynamodb")


def save_rate(table_name, rate_id, currency, buy, sale, source):
    table = dynamodb.Table(table_name)
    data = {
        "rateId": rate_id,
        "currency": currency,
        "buy": buy,
        "sale": sale,
        "source": source
    }
    print(f"Table: {table}")
    print(f"Data: {data}")

    response = table.put_item(Item=data)
    return response


def get_rate(table_name, rate_id):
    table = dynamodb.Table(table_name)
    response = table.get_item(Key={"rateId": rate_id})
    print(response)
    item = response["Item"]
    return item
