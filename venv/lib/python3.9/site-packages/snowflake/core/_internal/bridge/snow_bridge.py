# Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
import json
import re

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import ParseResult, urlparse

from urllib3 import HTTPResponse

from snowflake.connector import SnowflakeConnection
from snowflake.core._internal.bridge.resources.resource_base import ResourceBase

from .resources.computepools_resource import ComputePoolsResource
from .resources.database_resource import DatabaseResource
from .resources.imagerepo_resource import ImageRepositoriesResource
from .resources.schema_resource import SchemaResource
from .resources.services_resource import ServicesResource
from .resources.table_resource import TableResource
from .resources.task_resource import TaskResource
from .resources.warehouse_resource import WarehouseResource
from .rest_errors import BadRequest, InternalServerError, RestError
from .snow_request import SnowRequest


# List of resources supported by client bridge.
# Maps resource name and common prefix of all the URL's supported by this resource.
SUPPORTED_RESOURCES = {
    # TODO: Here order really matters for now, because SchemaResource
    #  will match everything that has that prefix in it
    TaskResource.resource_name: "/api/v2/databases/[^/]+/schemas/[^/]+/tasks(/[^/]+)*",
    ServicesResource.resource_name: "/api/v2/databases/[^/]+/schemas/[^/]+/services(/[^/]+)*",
    ImageRepositoriesResource.resource_name: "/api/v2/databases/[^/]+/schemas/[^/]+/image-repositories(/[^/]+)*",
    ComputePoolsResource.resource_name: "/api/v2/compute-pools(/[^/]+)*",
    TableResource.resource_name: "/api/v2/databases/[^/]+/schemas/[^/]+/tables(/[^/]+)*",
    WarehouseResource.resource_name: "/api/v2/warehouses(/[^/]+)*",
    SchemaResource.resource_name: "/api/v2/databases/[^/]+/schemas(/[^/]+)*",
    DatabaseResource.resource_name: "/api/v2/databases(/[^/]+)*",
}


class SnowBridge:
    """
    This acts as a request library for Snow APIs. It is a temporary solution until Snow APIs become available.
    """

    def __init__(self, conn: SnowflakeConnection):
        self.conn = conn

    def request(
        self,
        method: str,
        url: str,
        query_params: Optional[List[Tuple[str, Any]]] = None,
        headers: Optional[Dict[Any, Any]] = None,
        body: Optional[Dict[Any, Any]] = None,
        post_params: Optional[List[Tuple[str, Any]]] = None,
        _preload_content: Optional[bool] = None,
        _request_timeout: Optional[int] = None,
    ) -> HTTPResponse:
        try:
            return self._request_internal(
                method,
                url,
                query_params,
                headers,
                body,
                post_params,
                _preload_content,
                _request_timeout,
            )
        except RestError as e:
            return e.get_http_response()
        except Exception as e:
            return InternalServerError(str(e)).get_http_response()

    # TODO:
    # 1. Instantiate the appropriate resource based on resource_name
    def request_dispatcher(self, snow_request: SnowRequest) -> HTTPResponse:
        resource_name = self._get_resource_name(snow_request.url)
        resource: ResourceBase
        if resource_name == TaskResource.resource_name:
            resource = TaskResource(snow_request, self.conn)
        elif resource_name == SchemaResource.resource_name:
            resource = SchemaResource(snow_request, self.conn)
        elif resource_name == ServicesResource.resource_name:
            resource = ServicesResource(snow_request, self.conn)
        elif resource_name == ImageRepositoriesResource.resource_name:
            resource = ImageRepositoriesResource(snow_request, self.conn)
        elif resource_name == ComputePoolsResource.resource_name:
            resource = ComputePoolsResource(snow_request, self.conn)
        elif resource_name == TableResource.resource_name:
            resource = TableResource(snow_request, self.conn)
        elif resource_name == WarehouseResource.resource_name:
            resource = WarehouseResource(snow_request, self.conn)
        elif resource_name == DatabaseResource.resource_name:
            resource = DatabaseResource(snow_request, self.conn)
        else:
            raise BadRequest("Invalid URL")

        rows = resource.execute()
        return self._generate_http_response(rows)  # type: ignore[arg-type]

    def _request_internal(
        self,
        method: str,
        url: str,
        query_params: Optional[List[Tuple[str, Any]]] = None,
        headers: Optional[Dict[Any, Any]] = None,
        body: Optional[Dict[Any, Any]] = None,
        post_params: Optional[List[Tuple[str, Any]]] = None,
        _preload_content: Optional[bool] = None,
        _request_timeout: Optional[int] = None,
    ) -> HTTPResponse:
        # Perform validations:
        # Validate http method
        if method not in ("GET", "POST", "PUT", "DELETE"):
            raise ValueError(
                f"Invalid HTTP method '{method}', should be one of 'GET', 'POST', 'PUT', 'DELETE'."
            )
        # Validate URL
        parsed_url = self._validate_url(url)
        if query_params is not None and not isinstance(query_params, list):
            raise BadRequest("Invalid query_params, should be a list or None.")
        # Validate query_params
        if body is not None and not isinstance(body, dict):
            raise BadRequest("Invalid body, should be a dictionary or None.")
        # Validate headers
        if headers is not None and not isinstance(headers, dict):
            raise BadRequest("Invalid headers, should be a dictionary or None.")
        # Validate post_params
        if post_params is not None and not isinstance(post_params, list):
            raise BadRequest("Invalid post_params, should be a list or None.")

        if query_params is None:
            query_params = list()

        snow_request = SnowRequest(
            method=method,
            url=parsed_url,
            query_params=dict(query_params),
            headers=headers,
            body=body,
            post_params=dict(query_params),
            _preload_content=_preload_content,
            _request_timeout=_request_timeout,
        )

        return self.request_dispatcher(snow_request)

    def _get_resource_name(self, url: ParseResult) -> Optional[str]:
        """
        Extract the resource name from a given URL.
        url : str The URL string from which to extract the resource name.
        """
        path = url.path
        if path:
            for resource, url_pattern in SUPPORTED_RESOURCES.items():
                if re.match(url_pattern, path):
                    return resource

        return None

    def _validate_url(self, url: str) -> ParseResult:
        if not url or not isinstance(url, str):
            raise BadRequest("Invalid URL, should be a non-empty string.")

        parsed_url = urlparse(url)

        if not parsed_url.scheme or parsed_url.scheme.lower() not in ("http", "https"):
            raise BadRequest("Invalid URL scheme, should be 'http' or 'https'.")

        if not parsed_url.netloc:
            raise BadRequest("Invalid URL, missing domain.")

        if not parsed_url.path:
            raise BadRequest("Invalid URL, missing path.")

        return parsed_url

    def _generate_http_response(
        self,
        rows: Union[List[Dict[str, Any]], Dict[str, Any]],
    ) -> HTTPResponse:
        response = HTTPResponse(body=json.dumps(rows, cls=MyJsonEncoder))
        response.status = 200
        return response


class MyJsonEncoder(json.JSONEncoder):
    def default(self, z: Any) -> Any:
        if isinstance(z, datetime):
            return z.isoformat()
        else:
            return super().default(z)
