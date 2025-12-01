# Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
from abc import abstractmethod
from typing import Any, Dict, List, Tuple, Union
from urllib.parse import ParseResult

from snowflake.connector import SnowflakeConnection

from ..executor import SnowExecute


class ResourceBase:
    def __init__(self, conn_ob: SnowflakeConnection):
        self._conn_obj = conn_ob
        self.snow_exec = SnowExecute(conn_ob)

    def _parse_url(self, parsed_url: ParseResult) -> Tuple[str, str]:
        """
        url parser specific to ComputePools

        This function sets the stage to make SQL generation easier.
        It will find out if the action is on a collection or a resource.
        What's the name of a resource if its on a resource and if there's
        any custom action.
        """
        path = parsed_url.path
        path_wo_action = path
        action = ""
        if ":" in path:
            path_wo_action, _, action = path.rpartition(":")
        return action, path_wo_action

    @abstractmethod
    def execute(self) -> Union[List[Dict[str, Any]], Dict[str, Any], str, None]:
        ...
