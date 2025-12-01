# Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

import json

from logging import getLogger
from typing import Any, Dict, Optional

from urllib3 import HTTPResponse


logger = getLogger(__name__)


class RestError(Exception):
    """
    Exception class for REST API errors.
    """

    def __init__(
        self,
        msg: Optional[str] = None,
        status_code: int = 500,
        error_details: Optional[Dict[str, Any]] = None,
    ):
        if msg is None:
            self.msg = "Unknown Error"
        else:
            self.msg = msg
        self.error_details = error_details
        self.status_code = status_code

    def get_http_response(self) -> HTTPResponse:
        """
        Method to construct http response from the Exception object.
        """
        headers = {"Content-Type": "application/json"}
        json_str = json.dumps(
            {
                "error_code": str(self.status_code),
                "request_id": None,
                "message": f'{{error: "{self.msg}", details: "{self.error_details}"}}',
            }
        )
        return HTTPResponse(
            body=json_str.encode("utf-8"), status=self.status_code, headers=headers
        )


class BadRequest(RestError):
    def __init__(
        self,
        msg: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None,
    ) -> None:
        RestError.__init__(self, msg, 400, error_details)


class UnauthorizedRequest(RestError):
    def __init__(
        self,
        msg: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None,
    ) -> None:
        RestError.__init__(self, msg, 401, error_details)


class Forbidden(RestError):
    def __init__(
        self,
        msg: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None,
    ) -> None:
        RestError.__init__(self, msg, 403, error_details)


class NotFound(RestError):
    def __init__(
        self,
        msg: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None,
    ) -> None:
        RestError.__init__(self, msg, 404, error_details)


class Conflict(RestError):
    def __init__(
        self,
        msg: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None,
    ) -> None:
        RestError.__init__(self, msg, 409, error_details)


class InternalServerError(RestError):
    def __init__(
        self,
        msg: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None,
    ) -> None:
        RestError.__init__(self, msg, 500, error_details)


class BadGateway(RestError):
    def __init__(
        self,
        msg: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None,
    ) -> None:
        RestError.__init__(self, msg, 502, error_details)


class ServiceUnavailable(RestError):
    def __init__(
        self,
        msg: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None,
    ) -> None:
        RestError.__init__(self, msg, 503, error_details)


class GatewayTimeout(RestError):
    def __init__(
        self,
        msg: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None,
    ) -> None:
        RestError.__init__(self, msg, 504, error_details)
