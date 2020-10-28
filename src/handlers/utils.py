from uuid import uuid4


def generate_id(*args) -> str:
    return str(uuid4())
