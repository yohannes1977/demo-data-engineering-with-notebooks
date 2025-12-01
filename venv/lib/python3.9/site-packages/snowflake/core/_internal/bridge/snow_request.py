# Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
from typing import Any, Dict, Optional
from urllib.parse import ParseResult


class SnowRequest:
    """
    A class to represent a REST request.
    method : str
        The HTTP method to be used for the request (e.g., 'GET', 'POST', etc.).
    url : urllib.parse.ParseResult
        The URL of the API endpoint.
    query_params : dict
        A dictionary of query parameters to include in the request (default is None).
    headers : dict
        A dictionary of headers to include in the request (default is None).
    body : dict
        A dictionary representing the JSON payload to include in the request (default is None).
    post_params : dict
        A dictionary of POST parameters to include in the request (default is None).
    _preload_content : bool
        A boolean indicating whether to preload the response content (default is None).
    _request_timeout : int
        The timeout duration for the request in seconds (default is None).
    """

    def __init__(
        self,
        method: str,
        url: ParseResult,
        query_params: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[Any, Any]] = None,
        body: Optional[Dict[Any, Any]] = None,
        post_params: Optional[Dict[str, Any]] = None,
        _preload_content: Optional[bool] = None,
        _request_timeout: Optional[int] = None,
    ):
        self.method = method
        self.url = url
        self.query_params = query_params or {}
        self.headers = headers or {}
        self.body = body
        self.post_params = post_params or {}
        self._preload_content = _preload_content
        self._request_timeout = _request_timeout
