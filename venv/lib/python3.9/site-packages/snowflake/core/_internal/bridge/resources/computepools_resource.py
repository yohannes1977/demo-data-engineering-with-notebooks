# Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import unquote

from snowflake.connector import SnowflakeConnection

from ...utils import normalize_name, try_single_quote_value
from ..rest_errors import BadRequest, NotFound
from ..snow_request import SnowRequest
from .resource_base import ResourceBase


class ComputePoolsResource(ResourceBase):

    compute_pool_prop_list = {
        "required": ["name", "min_nodes", "max_nodes", "instance_family"],
        "optional": ["auto_resume"],
    }
    resource_name = "compute-pools"
    compute_pool_components = ["api", "v2", "compute-pools", "compute-pool-name"]

    def __init__(self, req: SnowRequest, conn_ob: SnowflakeConnection):
        super().__init__(conn_ob)
        self._action = ""
        self._is_collection = False
        self._pool_name = ""
        self._parameter_key = ""
        self._prop = req.body if req.body is not None else dict()
        self._query_params = req.query_params
        self._method = req.method

        self._action, path = self._parse_url(req.url)
        self._init_resource_metadata(path)

    def _init_resource_metadata(self, path: str) -> None:
        """
        Resource action metadata initializer

        This function will validate the URL and extract
        any metadata required to route to the right
        sql translate function.
        """
        path_parts = path.strip("/").split("/")
        self._is_collection = False
        if (
            len(path_parts)
            < ComputePoolsResource.compute_pool_components.index("compute-pools") + 1
        ):
            raise BadRequest("Malformed Resource URL")
        elif (
            len(path_parts)
            >= ComputePoolsResource.compute_pool_components.index("compute-pool-name")
            + 1
        ):
            self._pool_name = normalize_name(unquote(
                path_parts[
                    ComputePoolsResource.compute_pool_components.index(
                        "compute-pool-name"
                    )
                ]
            ))
        elif (
            len(path_parts)
            == ComputePoolsResource.compute_pool_components.index("compute-pools") + 1
        ):
            self._is_collection = True

    def execute(self) -> List[Dict[str, Any]] | Dict[str, Any]:
        """
        Routing function

        Using the VERB from the Rest request and the
        metadata collected from the call route to the
        appropriate SQL translation function.
        """
        ret: list[Dict[str, Any]] | Dict[str, Any] = list()
        if self._method == "PUT":
            _, ret = self.create_or_alter_cp()
        elif self._method == "GET":
            if self._is_collection:
                _, ret = self.show_cp()
            else:
                _, ret = self.desc_cp()
        elif self._method == "POST":
            if self._is_collection:
                _, ret = self.create_cp()
            elif self._action == "resume":
                _, ret = self.resume_cp()
            elif self._action == "suspend":
                _, ret = self.suspend_cp()
            elif self._action == "stopallservices":
                _, ret = self.stopallservices_cp()
        elif self._method == "DELETE":
            _, ret = self.drop_cp()
        else:
            raise BadRequest("Unsupported REST Verb used")
        return ret

    def create_or_alter_cp(self) -> tuple[str, Dict[str, Any]]:
        for key in ComputePoolsResource.compute_pool_prop_list["required"]:
            if key not in self._prop:
                raise BadRequest(f"Required property {key} is missing")

        if self._pool_name != normalize_name(self._prop["name"]):
            raise BadRequest("Property not consistent.")

        try:
            _, cp = self.desc_cp()
        except NotFound:
            return self.create_cp()
        coa_sql = [f"ALTER COMPUTE POOL {self._pool_name} SET"]
        for prop in self._prop:
            if prop == "name":
                # TODO Reconsider not throwing an error here as these props are immutable in an alter.
                continue
            if prop == "instance_family":
                if self._prop[prop] != cp[prop]:
                    raise BadRequest(
                        f"`instance_family` of a computer pool can't be changed. Trying to change from {cp[prop]} to {self._prop[prop]}"
                    )
                else:
                    continue
            if prop == "auto_resume":
                self._prop[prop] = "true" if self._prop[prop] else "false"
            if prop == "comment" and self._prop[prop]:
                self._prop[prop] = try_single_quote_value(self._prop[prop])
            coa_sql.append(f"{prop} = {self._prop[prop]}")
        if "auto_resume" not in self._prop:
            coa_sql.append("auto_resume = true")
        if "comment" not in self._prop:
            coa_sql.append("comment=null")
        coa_sql_s = " ".join(coa_sql)
        return coa_sql_s, self.snow_exec.execute(coa_sql_s)[0]

    def show_cp(self) -> tuple[str, List[Dict[str, Any]]]:
        sql_str = "SHOW COMPUTE POOLS "
        if "like" in self._query_params:
            sql_str = sql_str + "LIKE '{}' ".format(self._query_params["like"])
        if "startsWith" in self._query_params:
            sql_str = sql_str + "STARTS WITH '{}' ".format(
                self._query_params["startsWith"]
            )
        if "showLimit" in self._query_params:
            sql_str = sql_str + "LIMIT {} ".format(self._query_params["showLimit"])
            if "fromName" in self._query_params:
                sql_str = sql_str + "FROM '{}'".format(self._query_params["fromName"])

        resp = ComputePoolsResource._validate_desc_response(
            self.snow_exec.execute(sql_str)
        )
        return sql_str, resp

    def desc_cp(self) -> tuple[str, Dict[str, Any]]:
        sql_str = f"DESC COMPUTE POOL {self._pool_name}"

        resp = ComputePoolsResource._validate_desc_response(
            self.snow_exec.execute(sql_str)
        )

        return sql_str, resp[0]

    def create_cp(self) -> tuple[str, Dict[str, Any]]:
        sql_str = "CREATE COMPUTE POOL "
        if (
            "ifNotExists" in self._query_params
            and self._query_params["ifNotExists"].lower() == "true"
        ):
            sql_str = sql_str + "IF NOT EXISTS "
        if "name" not in self._prop:
            raise BadRequest("Name is a required field for Creating a Compute Pool")
        sql_str = sql_str + self._prop["name"] + " "

        if "min_nodes" in self._prop:
            sql_str = sql_str + "MIN_NODES = {} ".format(self._prop["min_nodes"])
        if "max_nodes" in self._prop:
            sql_str = sql_str + "MAX_NODES = {} ".format(self._prop["max_nodes"])

        if "instance_family" in self._prop:
            sql_str = sql_str + "INSTANCE_FAMILY = '{}' ".format(
                self._prop["instance_family"]
            )

        if "auto_resume" in self._prop:
            # TODO check if the value of auto resume is anything besides true | false.
            sql_str = sql_str + "AUTO_RESUME = {} ".format(
                str(self._prop["auto_resume"]).lower()
            )
        if "comment" in self._prop:
            sql_str = sql_str + "COMMENT = {} ".format(
                try_single_quote_value(self._prop["comment"])
            )

        return sql_str, self.snow_exec.execute(sql_str)[0]

    def suspend_cp(self) -> tuple[str, Dict[str, Any]]:
        sql_str = f"ALTER COMPUTE POOL {self._pool_name} SUSPEND"
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def resume_cp(self) -> tuple[str, Dict[str, Any]]:
        sql_str = f"ALTER COMPUTE POOL {self._pool_name} RESUME"
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def stopallservices_cp(self) -> tuple[str, Dict[str, Any]]:
        sql_str = f"ALTER COMPUTE POOL {self._pool_name} STOP ALL"
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def drop_cp(self) -> tuple[str, Dict[str, Any]]:
        if "ifExists" in self._query_params and self._query_params["ifExists"]:
            sql_str = f"DROP COMPUTE POOL IF EXISTS {self._pool_name}"
        else:
            sql_str = f"DROP COMPUTE POOL {self._pool_name} "
        return sql_str, self.snow_exec.execute(sql_str)[0]

    @staticmethod
    def _validate_desc_response(resp: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        for row in resp:

            if "auto_resume" in row:
                row["auto_resume"] = True if row["auto_resume"] == "true" else False
        return resp
