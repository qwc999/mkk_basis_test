import json
import math
from decimal import Decimal

from fastapi import Request, Response
from fastapi.routing import APIRoute


def reject_nonfinite_number(token: str):
    raise json.JSONDecodeError("Non-finite JSON number", token, 0)


def parse_metadata_number(token: str) -> float:
    number = float(token)
    if not math.isfinite(number):
        reject_nonfinite_number(token)
    return number


class PaymentJSONRequest(Request):
    async def json(self):
        body = await self.body()
        data = json.loads(body, parse_float=Decimal, parse_constant=reject_nonfinite_number)
        if isinstance(data, dict) and "metadata" in data:
            # Only the monetary amount needs exact decimal parsing. Metadata
            # keeps ordinary JSON types supported by Pydantic and JSONB.
            data["metadata"] = json.loads(
                body, parse_float=parse_metadata_number, parse_constant=reject_nonfinite_number
            )["metadata"]
        return data


class PaymentJSONRoute(APIRoute):
    def get_route_handler(self):
        original_handler = super().get_route_handler()

        async def handler(request: Request) -> Response:
            return await original_handler(PaymentJSONRequest(request.scope, request.receive))

        return handler
