# Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
from __future__ import annotations

from operator import methodcaller
from typing import Any, Dict, List
from urllib.parse import unquote

from snowflake.connector import SnowflakeConnection

from ...utils import normalize_name, try_single_quote_value, unquote_name
from ..rest_errors import BadRequest, NotFound
from ..snow_request import SnowRequest
from .resource_base import ResourceBase


class SchemaResource(ResourceBase):

    schema_prop_list = {
        "required": ["name",],
        "optional": [
            "comment",
            # read/write parameters
            "database_name",
            "data_retention_time_in_days",
            "default_ddl_collation",
            "log_level",
            "pipe_execution_paused",
            "max_data_extension_time_in_days",
            "suspend_task_after_num_failures",
            "trace_level",
            "user_task_managed_initial_warehouse_size",
            "user_task_timeout_ms",
        ]
    }
    resource_name = "schemas"
    schema_components = ["api", "v2", "databases", "database-name", "schemas", "schema-name", "property"]

    def __init__(self, req: SnowRequest, conn_ob: SnowflakeConnection):
        super().__init__(conn_ob)
        self._action = ""
        self._is_collection = False
        self._property = ""
        self._db_name = ""
        self._schema_name = ""
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
        self._db_name = normalize_name(unquote(
            path_parts[
                SchemaResource.schema_components.index(
                    "database-name"
                )
            ]
        ))
        if (
            len(path_parts)
            < SchemaResource.schema_components.index("schemas") + 1
        ):
            raise BadRequest("Malformed Resource URL")
        elif (
            len(path_parts)
            >= SchemaResource.schema_components.index("schema-name")
            + 1
        ):
            self._schema_name = normalize_name(unquote(
                path_parts[
                    SchemaResource.schema_components.index(
                        "schema-name"
                    )
                ]
            ))
        elif (
            len(path_parts)
            == SchemaResource.schema_components.index("schemas") + 1):
            self._is_collection = True
        if (
            len(path_parts)
            == SchemaResource.schema_components.index("property") + 1
        ):
            self._property = path_parts[
                    SchemaResource.schema_components.index(
                        "property"
                    )
                ]


    def execute(self) -> List[Dict[str, Any]] | Dict[str, Any]:
        """
        Routing function

        Using the VERB from the Rest request and the
        metadata collected from the call route to the
        appropriate SQL translation function.
        """
        ret: list[Dict[str, Any]] | Dict[str, Any] = list()
        if self._method == "PUT":
            _, ret = self.create_or_alter_schema()
        elif self._method == "GET":
            if self._is_collection:
                _, ret = self.show_schemas()
            else:
                _, ret = self.desc_schema()
        elif self._method == "POST":
            if self._action == "clone":
                _, ret = self.clone_schema()
            elif self._is_collection:
                _, ret = self.create_schema()
            else:
                raise BadRequest(
                    f"Unsupported property '{self._property}' for action''"
                )
        elif self._method == "DELETE":
            _, ret = self.drop_schema()
        else:
            raise BadRequest("Unsupported REST Verb used")
        return ret

    def create_or_alter_schema(self) -> tuple[str, Dict[str, Any]]:
        for key in SchemaResource.schema_prop_list["required"]:
            if key not in self._prop:
                raise BadRequest(f"Required property {key} is missing")

        if self._schema_name != normalize_name(self._prop["name"]):
            raise BadRequest(f"Schema names are not consistent, {self._schema_name} != {self._prop['name']}.")

        try:
            _, db = self.desc_schema()
        except NotFound:
            return self.create_schema()
        coa_sql = [f"ALTER SCHEMA {self._db_name}.{self._schema_name} SET",]
        for prop in self._prop:
            prop_v = self._prop[prop]
            if prop_v == db.get(prop):
                continue
            if prop == "name":
                continue
            elif prop in (
                "created_on",
                "is_default",
                "is_current",
                "origin",
                "owner",
                "options",
                "dropped_on",
                "owner_role_type",
            ):
                raise BadRequest(
                    f"`{prop}` of a schema can't be changed as it is read-only"
                )
            elif prop in (
                # Parameters that are strings
                "comment",
                "default_ddl_collation",
            ) and prop_v:
                prop_v = try_single_quote_value(prop_v)
            coa_sql.append(f"{prop} = {prop_v}")
        # TODO: should other props be unset if they're missing?
        if "comment" not in self._prop or self._prop["comment"] is None:
            coa_sql.append("comment=null")
        if len(coa_sql) == 1:
            # Nothing is being updated, skip executing anything
            return ("", dict())
        coa_sql_s = " ".join(coa_sql)
        return coa_sql_s, self.snow_exec.execute(coa_sql_s)[0]

    def show_schemas(self) -> tuple[str, List[Dict[str, Any]]]:
        sql_str = "SHOW SCHEMAS "
        if "history" in self._query_params:
            sql_str = sql_str + "HISTORY "
        if "like" in self._query_params:
            sql_str = sql_str + "LIKE '{}' ".format(self._query_params["like"])
        sql_str += f"IN DATABASE {self._db_name} "
        if "startsWith" in self._query_params:
            sql_str = sql_str + "STARTS WITH '{}' ".format(
                self._query_params["startsWith"]
            )
        if "showLimit" in self._query_params:
            sql_str = sql_str + "LIMIT {} ".format(self._query_params["showLimit"])
        if "fromName" in self._query_params:
            sql_str = sql_str + "FROM '{}'".format(self._query_params["fromName"])

        resp = self.snow_exec.execute(sql_str)
        # White list to select columns out of show command
        ret: List[Dict[str, Any]] = [
            {
                k: v for k, v in r.items() if k in (
                    "created_on",
                    "name",
                    "is_default",
                    "is_current",
                    "database_name",
                    "owner",
                    "comment",
                    "options",
                    "owner_role_type",
                )
            } for r in resp
        ]
        final_results = []
        for schema in ret:
            params_sql = f"SHOW PARAMETERS IN SCHEMA {self._db_name}.{normalize_name(schema['name'])}"
            try:
                params_resp = self.snow_exec.execute(params_sql)
            except NotFound:
                continue
            params_dict = {d["key"]: d["value"] for d in params_resp}
            for n in tuple(map(methodcaller('upper'), self.schema_prop_list['optional'][1:])):
                if n in params_dict:
                    schema[n.lower()] = params_dict.get(n)
            final_results.append(schema)
        return sql_str, self._transform_show_responses(final_results)

    def desc_schema(self) -> tuple[str, Dict[str, Any]]:
        self._query_params["like"] = unquote_name(self._schema_name)
        sql_str, resp = self.show_schemas()
        if not resp:
            raise NotFound("Schema cannot be found.")
        for schema in resp:
            if normalize_name(schema["name"]) == self._schema_name:
                return sql_str, resp[0]
        raise NotFound("Schema cannot be found.")

    def create_schema(self) -> tuple[str, Dict[str, Any]]:
        sql_str = "CREATE "
        createMode = self._query_params.get("createMode", "")
        if createMode == "orReplace":
            sql_str += "OR REPLACE "
        kind = self._query_params.get("kind", "")
        if kind:
            sql_str += kind + " "
        sql_str += "SCHEMA "
        if createMode == "ifNotExists":
            sql_str += "IF NOT EXISTS "
        if "name" not in self._prop:
            raise BadRequest("Name is a required field for creating a schema")
        if self._db_name:
            sql_str += self._db_name + "."
        sql_str += normalize_name(self._prop["name"]) + " "
        managed_access = self._query_params.get("with_managed_access")
        if managed_access:
            sql_str += "WITH MANAGED ACCESS "
        for k, single_quote in (
            ("data_retention_time_in_days", False),
            ("max_data_extension_time_in_days", False),
            ("default_ddl_collation", True),
            ("comment", True),
            ("log_level", False),
            ("pipe_execution_paused", False),
            ("suspend_task_after_num_failures", False),
            ("trace_level", False),
            ("user_task_managed_initial_warehouse_size", True),
            ("user_task_timeout_ms", False),
        ):
            if k in self._prop:
                v = self._prop[k]
                if single_quote:
                    v = try_single_quote_value(v)
                sql_str += f"{k.upper()} = {v} "

        return sql_str, self.snow_exec.execute(sql_str)[0]

    def clone_schema(self) -> tuple[str, Dict[str, Any]]:
        sql_str = "CREATE "
        createMode = self._query_params.get("createMode", "")
        if createMode == "orReplace":
            sql_str += "OR REPLACE "
        kind = self._query_params.get("kind", "")
        if kind:
            sql_str += kind + " "
        sql_str += "SCHEMA "
        if createMode == "ifNotExists":
            sql_str += "IF NOT EXISTS "
        if "name" not in self._prop:
            raise BadRequest("Name is a required field for cloning a schema")
        # TODO: this doesn't work anymore with the new clone URL
        if self._db_name:
            sql_str += self._db_name + "."
        sql_str += normalize_name(self._prop["name"]) + " "
        clone: str | None = self._prop["name"]
        sql_str += f"CLONE {normalize_name(self._schema_name)} "
        pot = self._prop.get("point_of_time")
        if pot is not None:
            sql_str += f"{pot['reference'].upper()} ({pot['point_of_time_type'].upper()} => {pot[pot['point_of_time_type']]}) "
        managed_access = self._query_params.get("with_managed_access")
        if managed_access:
            sql_str += "WITH MANAGED ACCESS "
        for k, single_quote in (
            ("data_retention_time_in_days", False),
            ("max_data_extension_time_in_days", False),
            ("default_ddl_collation", True),
            ("comment", True),
            ("log_level", False),
            ("pipe_execution_paused", False),
            ("suspend_task_after_num_failures", False),
            ("trace_level", False),
            ("user_task_managed_initial_warehouse_size", True),
            ("user_task_timeout_ms", False),
        ):
            if k in self._prop:
                v = self._prop[k]
                if single_quote:
                    v = try_single_quote_value(v)
                sql_str += f"{k.upper()} = {v} "

        return sql_str, self.snow_exec.execute(sql_str)[0]

    def drop_schema(self) -> tuple[str, Dict[str, Any]]:
        sql_str = "DROP SCHEMA "
        if self._query_params.get("createMode", "") == "ifExists":
            sql_str += "IF EXISTS "
        sql_str += f"{self._db_name}.{self._schema_name}"
        return sql_str, self.snow_exec.execute(sql_str)[0]

    @staticmethod
    def _transform_show_response(resp: Dict[str, Any]) -> Dict[str, Any]:
        for k in (
            "is_default",
            "is_current",
        ):
            resp[k] = (resp[k].upper() == "Y")
        for k in (
            "pipe_execution_paused",
        ):
            resp[k] = (resp[k].upper() == "TRUE")
        for k in (
            "data_retention_time_in_days",
            "max_data_extension_time_in_days",
            "suspend_task_after_num_failures",
            "user_task_timeout_ms",
        ):
            resp[k] = None if k not in resp else int(resp[k])
        if resp["comment"] == "":
            resp["comment"] = None
        resp["name"] = normalize_name(resp["name"])
        return resp

    @staticmethod
    def _transform_show_responses(resp: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return list(map(SchemaResource._transform_show_response, resp))
