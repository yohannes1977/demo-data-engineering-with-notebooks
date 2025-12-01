# Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
from __future__ import annotations

from typing import Any, Dict, List, Union
from urllib.parse import unquote

from snowflake.connector import SnowflakeConnection
from snowflake.core._common import _is_an_update

from ...utils import normalize_name, try_single_quote_value
from ..rest_errors import BadRequest, NotFound
from ..snow_request import SnowRequest
from .resource_base import ResourceBase


class WarehouseResource(ResourceBase):
    warehouse_prop_list = {
        "required": ["name"],
        "optional": [
            "warehouse_type",
            "warehouse_size",
            "wait_for_completion",
            "max_cluster_count",
            "min_cluster_count",
            "scaling_policy",
            "auto_suspend",
            "auto_resume",
            "initially_suspended",
            "resource_monitor",
            "comment",
            "enable_query_acceleration",
            "query_acceleration_max_scale_factor",
            "max_concurrency_level",
            "statement_queued_timeout_in_seconds",
            "statement_timeout_in_seconds",
        ],
        "alter_warehouse_option": [
            # "wait_for_completion",
            "max_cluster_count",
            "min_cluster_count",
            "auto_suspend",
            "auto_resume",
            # "resource_monitor",
            "comment",
            "enable_query_acceleration",
            # "query_acceleration_max_scale_factor",
            "max_concurrency_level",
            "statement_queued_timeout_in_seconds",
            "statement_timeout_in_seconds",
        ],
        "objectParameters": [
            "max_concurrency_level",
            "statement_queued_timeout_in_seconds",
            "statement_timeout_in_seconds",
        ]
    }
    resource_name = "warehouses"
    warehouse_components = ["api", "v2", "warehouses", "warehouse-name"]

    def __init__(self, req: SnowRequest, conn_ob: SnowflakeConnection):
        super().__init__(conn_ob)
        self._warehouse_name = ""
        self._is_collection = False
        self._prop = req.body if req.body is not None else dict()
        self._query_params = req.query_params
        self._method = req.method
        self._action, path = self._parse_url(req.url)
        self._init_resource_metadata(path)
        self.object_para_pair = {
                "NUMBER": int,
                "STRING": str,
                "BOOLEAN": bool
            }

    def _init_resource_metadata(self, path: str) -> None:
        """
        Resource action metadata initializer

        This function will validate the URL and extract
        any metadata required to route to the right
        sql translate function.
        """
        path_parts = path.strip("/").split("/")
        self._is_collection = False
        if len(path_parts) < WarehouseResource.warehouse_components.index("warehouses") + 1:
            raise BadRequest("Malformed Resource URL")
        elif self._warehouse_name == "" and len(path_parts) >= WarehouseResource.warehouse_components.index("warehouse-name") + 1:
            self._warehouse_name = normalize_name(unquote(path_parts[WarehouseResource.warehouse_components.index("warehouse-name")]))
        elif len(path_parts) == WarehouseResource.warehouse_components.index("warehouses") + 1:
            self._is_collection = True

    def execute(self) -> Union[List[Dict[str, Any]], Dict[str, Any], str, None]:
        """
        Routing function

        Using the VERB from the Rest request and the
        metadata collected from the call route to the
        appropriate SQL translation function.
        """
        ret: list[Dict[str, Any]] | Dict[str, Any] = list()
        if self._method == "PUT":
            _, ret = self.create_or_update_warehouse()
        elif self._method == "GET":
            if self._is_collection:
                _, ret = self.list_warehouses()
            else:
                _, ret = self.fetch_warehouse()
        elif self._method == "POST":
            if self._is_collection:
                _, ret = self.create_or_update_warehouse()
            elif self._action == "resume":
                _, ret = self.resume_warehouse()
            elif self._action == "suspend":
                _, ret = self.suspend_warehouse()
            elif self._action == "abort":
                _, ret = self.abort_all_queries_warehouse()
            elif self._action == "rename":
                _, ret = self.rename_warehouse()
        elif self._method == "DELETE":
            _, ret = self.drop_warehouse()
        else:
            raise BadRequest("Unsupported REST Verb used")

        return ret

    def update_and_set_warehouse(self) -> tuple[str, Dict[str, Any]]:
        for key in WarehouseResource.warehouse_prop_list["required"]:
            if key not in self._prop:
                raise BadRequest(f"{key} is a required field for Updating a Warehouse")
        if self._warehouse_name != normalize_name(self._prop["name"]):
            raise BadRequest("Property name not consistent. Use rename if you want to change warehouse's name.")
        if not _is_an_update(self._prop):
            raise BadRequest("Can not alter warehouse set with no parameters")
        sql_str = "ALTER WAREHOUSE "
        sql_str += self._prop["name"] + " "
        sql_str += "SET "
        for key in WarehouseResource.warehouse_prop_list["optional"]:
            if key in self._prop:
                prop_v = self._prop[key]
                if key in ("comment",) and prop_v:
                    prop_v = try_single_quote_value(prop_v)
                sql_str += f"{key.upper()} = {prop_v} "

        return sql_str, self.snow_exec.execute(sql_str)[0]

    def update_and_unset_warehouse(self) -> tuple[str, Dict[str, Any]]:
        for key in WarehouseResource.warehouse_prop_list["required"]:
            if key not in self._prop:
                raise BadRequest(f"{key} is a required field for Updating a Warehouse")
        if self._warehouse_name != normalize_name(self._prop["name"]):
            raise BadRequest("Property name not consistent.")
        if not _is_an_update(self._prop):
            raise BadRequest("Can not alter warehouse unset with no parameters")
        sql_str = "ALTER WAREHOUSE "
        sql_str += self._prop["name"] + " "
        sql_str += "UNSET "
        unset_parameters = list(set(WarehouseResource.warehouse_prop_list["alter_warehouse_option"]).difference(set(self._prop)))
        for i, key in enumerate(unset_parameters):
            if key in WarehouseResource.warehouse_prop_list["alter_warehouse_option"]:
                sql_str += f"{key.upper()}"
                if i != len(unset_parameters)-1:
                    sql_str += ", "

        return sql_str, self.snow_exec.execute(sql_str)[0]

    def list_warehouses(self) -> tuple[str, List[Dict[str, Any]]]:
        sql_str = "SHOW WAREHOUSES "
        if "like" in self._query_params:
            sql_str += "LIKE '{}'".format(self._query_params["like"])
        results = self.snow_exec.execute(sql_str)
        final_results = []
        for res in results:
            obj_para_sql = "SHOW PARAMETERS IN WAREHOUSE {}".format(res["name"])
            try:
                # 2023-10-20(bwarsaw): It is possible that we see a warehouse returned by SHOW WAREHOUES for
                # which we do not have the permission to retrieve its parameters.  What to do? It's not
                # appropriate to just let the 400 Bad Request get propagated up, because it essentially means
                # that there are environments (such as our as of this writing, unclean test environment) where
                # you cannot get a list of warehouses.  The least worst thing seems to be to catch the
                # exception and leave the body of the warehouse representation (i.e. JSON) be empty.
                object_parameters = self.snow_exec.execute(obj_para_sql)
            except (BadRequest, NotFound):
                continue

            for object_parameter in object_parameters:
                value  = (
                    self.object_para_pair[object_parameter["type"]](object_parameter["value"])
                    if object_parameter["level"] == "WAREHOUSE"
                    else None)
                res[object_parameter["key"].lower()] = value
            final_results.append(res)
        return sql_str, self._transform_warehouses(final_results)

    @staticmethod
    def _transform_warehouses(warehouses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        for w in warehouses:
            # Turn is_default and is_current to booleans
            for e in ('is_default', 'is_current'):
                if e in w:
                    w[e] = (w[e] == 'Y')
            if w["comment"] == "":
                w["comment"] = None
        return warehouses

    def describe_warehouse(self) -> tuple[str, Dict[str, Any]]:
        sql_str = f"DESC WAREHOUSE {self._warehouse_name}"
        res = self.snow_exec.execute(sql_str)[0]
        obj_para_sql = f"SHOW PARAMETERS IN WAREHOUSE {self._warehouse_name}"
        object_parameters = self.snow_exec.execute(obj_para_sql)
        for object_parameter in object_parameters:
            if object_parameter["level"] == "WAREHOUSE":
                res[object_parameter["key"].lower()] = int(object_parameter["value"])
            else:
                res[object_parameter["key"].lower()] = None
        return sql_str, res

    def fetch_warehouse(self) -> tuple[str, Dict[str, Any]]:
        _, des_res = self.describe_warehouse()
        self._query_params["like"] = self._warehouse_name
        _, show_results = self.list_warehouses()
        duplicate = 0
        for result in show_results:
            if result["name"] == self._warehouse_name:
                duplicate += 1
        assert duplicate <= 1
        show_res = show_results[0]
        result = {}
        for para in des_res:
            if para in show_res and show_res[para] != des_res[para]:
                raise BadRequest("ERROR: 500 show and describe result doesnt match")
        result.update(des_res)
        result.update(show_res)
        return "", result

    def create_or_replace_warehouse(self) -> tuple[str, Dict[str, Any]]:
        sql_str = "CREATE "

        if "createMode" in self._query_params:
            create_mode = self._query_params["createMode"].lower()
            if create_mode == "errorifexists":
                sql_str += "WAREHOUSE "
            elif create_mode == "ifnotexists":
                sql_str += "WAREHOUSE IF NOT EXISTS "
            elif create_mode == "orreplace":
                sql_str += "OR REPLACE WAREHOUSE "
        else:
            sql_str += "WAREHOUSE "

        if "name" not in self._prop:
            raise BadRequest("Name is a required field for Creating a Warehouse")
        sql_str += self._prop["name"] + " "

        for key in filter(
            lambda e: e in self._prop,
            WarehouseResource.warehouse_prop_list["optional"],
        ):
            v = self._prop[key]
            if key == "comment":
                sql_str += f" COMMENT = {try_single_quote_value(v)}, "
            else:
                sql_str += f"{key.upper()} = {v} "
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def create_or_update_warehouse(self) -> tuple[str, Dict[str, Any]]:
        show_sql = "SHOW WAREHOUSES LIKE '{}'".format(self._prop["name"])
        show_result = self.snow_exec.execute(show_sql)
        exist = False
        for exist_warehouse in show_result:
            if self._prop["name"].upper() == exist_warehouse["name"].upper():
                self._warehouse_name = normalize_name(self._prop["name"])
                exist = True
                break
        if exist:
            if not _is_an_update(self._prop):
                return "", {"description": "command ignored cause this is not an update"}
            _, ret = self.update_and_set_warehouse()
            _, ret = self.update_and_unset_warehouse()
        else:
            _, ret = self.create_or_replace_warehouse()
        return _, ret

    def resume_warehouse(self) -> tuple[str, Dict[str, Any]]:
        sql_str = "ALTER WAREHOUSE "
        sql_str += self._warehouse_name + " RESUME"
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def suspend_warehouse(self) -> tuple[str, Dict[str, Any]]:
        sql_str = "ALTER WAREHOUSE "
        sql_str += self._warehouse_name + " SUSPEND"
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def abort_all_queries_warehouse(self) -> tuple[str, Dict[str, Any]]:
        sql_str = "ALTER WAREHOUSE "

        sql_str += self._warehouse_name + " ABORT ALL QUERIES"
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def rename_warehouse(self) -> tuple[str, Dict[str, Any]]:
        sql_str = "ALTER WAREHOUSE "
        if "name" not in self._prop:
            raise BadRequest("New warehouse name is a required field for renaming a Warehouse")
        sql_str += self._warehouse_name + " RENAME TO " + normalize_name(self._prop["name"])

        self._warehouse_name = normalize_name(self._prop["name"])
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def drop_warehouse(self) -> tuple[str, Dict[str, Any]]:
        sql_str = "DROP WAREHOUSE "
        sql_str += self._warehouse_name
        return sql_str, self.snow_exec.execute(sql_str)[0]
