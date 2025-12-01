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


class DatabaseResource(ResourceBase):

    database_prop_list = {
        "required": ["name",],
        "optional": [
            "comment",
            # read/write parameters
            "data_retention_time_in_days",
            "default_ddl_collation",
            "log_level",
            "max_concurrency_level",
            "max_data_extension_time_in_days",
            "suspend_task_after_num_failures",
            "trace_level",
            "user_task_managed_initial_warehouse_size",
            "user_task_timeout_ms",
        ]
    }
    resource_name = "databases"
    database_components = ["api", "v2", "databases", "database-name", "property"]

    def __init__(self, req: SnowRequest, conn_ob: SnowflakeConnection):
        super().__init__(conn_ob)
        self._action = ""
        self._is_collection = False
        self._property = ""
        self._db_name = ""
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
            < DatabaseResource.database_components.index("databases") + 1
        ):
            raise BadRequest("Malformed Resource URL")
        elif (
            len(path_parts)
            >= DatabaseResource.database_components.index("database-name")
            + 1
        ):
            self._db_name = normalize_name(unquote(
                path_parts[
                    DatabaseResource.database_components.index(
                        "database-name"
                    )
                ]
            ))
        elif (
            len(path_parts)
            == DatabaseResource.database_components.index("databases") + 1
        ):
            self._is_collection = True
        if (
            len(path_parts)
            == DatabaseResource.database_components.index("property") + 1
        ):
            self._property = path_parts[
                    DatabaseResource.database_components.index(
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
            _, ret = self.create_or_alter_db()
        elif self._method == "GET":
            if self._is_collection:
                _, ret = self.show_db()
            else:
                _, ret = self.desc_db()
        elif self._method == "POST":
            if self._action == "" and self._is_collection:
                _, ret = self.create_db()
            elif self._action == "clone":
                    _, ret = self.clone_db()
            elif self._action == 'from_share':
                _, ret = self.create_db_from_share()
            elif self._action == "enable":
                if self._property == "replication":
                    _, ret = self.enable_replication()
                elif self._property == "failover":
                    _, ret = self.enable_failover()
                else:
                    raise BadRequest(
                        f"Unsupported property '{self._property}' for action 'enable'"
                    )
            elif self._action == "disable":
                if self._property == "replication":
                    _, ret = self.disable_replication()
                elif self._property == "failover":
                    _, ret = self.disable_failover()
                else:
                    raise BadRequest(
                        f"Unsupported property '{self._property}' for action 'disable'"
                    )
            elif self._action == "refresh":
                _, ret = self.refresh_replication()
            elif self._action == "primary":
                _, ret = self.primary_failover()
            else:
                raise BadRequest(
                    f"Unsupported action '{self._action}' while POSTing"
                )


        elif self._method == "DELETE":
            _, ret = self.drop_db()
        else:
            raise BadRequest("Unsupported REST Verb used")
        return ret

    def create_or_alter_db(self) -> tuple[str, Dict[str, Any]]:
        for key in DatabaseResource.database_prop_list["required"]:
            if key not in self._prop:
                raise BadRequest(f"Required property {key} is missing")

        if self._db_name != normalize_name(self._prop["name"]):
            raise BadRequest(f"Database names are not consistent, {self._db_name} != {self._prop['name']}.")

        try:
            _, db = self.desc_db()
        except NotFound:
            return self.create_db()
        coa_sql = [f"ALTER DATABASE {self._db_name} SET",]
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
                    f"`{prop}` of a database can't be changed as it is read-only"
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

    def show_db(self) -> tuple[str, List[Dict[str, Any]]]:
        sql_str = "SHOW DATABASES "
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

        resp = self.snow_exec.execute(sql_str)
        # White list to select columns out of show command
        ret: List[Dict[str, Any]] = [
            {
                k: v for k, v in r.items() if k in (
                    "created_on",
                    "name",
                    "is_default",
                    "is_current",
                    "origin",
                    "owner",
                    "comment",
                    "options",
                    "dropped_on",
                )
            } for r in resp
        ]
        final_results = []
        for db in ret:
            params_sql = f"SHOW PARAMETERS IN DATABASE {normalize_name(db['name'])}"
            try:
                params_resp = self.snow_exec.execute(params_sql)
            except NotFound:
                continue
            params_dict = {d["key"]: d["value"] for d in params_resp}
            for n in tuple(map(methodcaller('upper'), DatabaseResource.database_prop_list['optional'][1:])):
                if n in params_dict:
                    db[n.lower()] = params_dict.get(n)
            final_results.append(db)
        return sql_str, self._transform_show_responses(final_results)

    def desc_db(self) -> tuple[str, Dict[str, Any]]:
        self._query_params["like"] = unquote_name(self._db_name)
        sql_str, resp = self.show_db()
        if not resp:
            raise NotFound("Database cannot be found.")
        for db in resp:
            if normalize_name(db["name"]) == normalize_name(self._db_name):
                return sql_str, db
        raise NotFound("Database cannot be found.")

    def create_db(self) -> tuple[str, Dict[str, Any]]:
        sql_str = "CREATE "
        createMode = self._query_params.get("createMode", "")
        if createMode == "orReplace":
            sql_str += "OR REPLACE "
        kind = self._query_params.get("kind", "")
        if kind:
            sql_str += kind + " "
        sql_str += "DATABASE "
        if createMode == "ifNotExists":
            sql_str += "IF NOT EXISTS "
        if "name" not in self._prop:
            raise BadRequest("Name is a required field for creating a database")
        sql_str = sql_str + normalize_name(self._prop["name"]) + " "
        if "data_retention_time_in_days" in self._prop:
            sql_str = sql_str + "DATA_RETENTION_TIME_IN_DAYS = {} ".format(
                self._prop["data_retention_time_in_days"]
            )
        if "max_data_extension_time_in_days" in self._prop:
            sql_str = sql_str + "MAX_DATA_EXTENSION_TIME_IN_DAYS = {} ".format(
                self._prop["max_data_extension_time_in_days"]
            )
        if "default_ddl_collation" in self._prop and self._prop["default_ddl_collation"]:
            sql_str = sql_str + "DEFAULT_DDL_COLLATION = {} ".format(
                try_single_quote_value(self._prop["default_ddl_collation"])
            )

        if "comment" in self._prop:
            sql_str = sql_str + "COMMENT = {} ".format(
                try_single_quote_value(self._prop["comment"])
            )

        return sql_str, self.snow_exec.execute(sql_str)[0]

    def clone_db(self) -> tuple[str, Dict[str, Any]]:
        sql_str = "CREATE "
        createMode = self._query_params.get("createMode", "")
        if createMode == "orReplace":
            sql_str += "OR REPLACE "
        kind = self._query_params.get("kind", "")
        if kind:
            sql_str += kind + " "
        sql_str += "DATABASE "
        if createMode == "ifNotExists":
            sql_str += "IF NOT EXISTS "
        if "name" not in self._prop:
            raise BadRequest("Name is a required field for cloning a database")
        sql_str = sql_str + normalize_name(self._prop["name"]) + " "
        clone: str | None = self._prop.get("name")
        if clone is not None:
            sql_str += f"CLONE {normalize_name(self._db_name)} "
            pot = self._prop.get("point_of_time")
            if pot is not None:
                sql_str += f"{pot['reference'].upper()} ({pot['point_of_time_type'].upper()} => {pot[pot['point_of_time_type']]}) "
        if "data_retention_time_in_days" in self._prop:
            sql_str = sql_str + "DATA_RETENTION_TIME_IN_DAYS = {} ".format(
                self._prop["data_retention_time_in_days"]
            )
        if "max_data_extension_time_in_days" in self._prop:
            sql_str = sql_str + "MAX_DATA_EXTENSION_TIME_IN_DAYS = {} ".format(
                self._prop["max_data_extension_time_in_days"]
            )
        if "default_ddl_collation" in self._prop and self._prop["default_ddl_collation"]:
            sql_str = sql_str + "DEFAULT_DDL_COLLATION = {} ".format(
                try_single_quote_value(self._prop["default_ddl_collation"])
            )

        if "comment" in self._prop:
            sql_str = sql_str + "COMMENT = {} ".format(
                try_single_quote_value(self._prop["comment"])
            )

        return sql_str, self.snow_exec.execute(sql_str)[0]

    def create_db_from_share(self) -> tuple[str, Dict[str, Any]]:
        sql_str = "CREATE "
        createMode = self._query_params.get("createMode")
        if createMode is not None:
            if createMode == "orReplace":
                sql_str += "OR REPLACE "
            elif createMode == "ifNotExists":
                sql_str += "IF NOT EXISTS "
        options = self._prop.get("options")
        sql_str += "DATABASE "
        sql_str += f"{self._db_name} FROM SHARE {self._query_params['share']} "

        return sql_str, self.snow_exec.execute(sql_str)[0]

    def drop_db(self) -> tuple[str, Dict[str, Any]]:
        sql_str = "DROP DATABASE "
        createMode = self._query_params.get("createMode")
        if self._query_params.get("createMode", "") == "ifExists":
                sql_str += "IF EXISTS "
        sql_str += self._db_name
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def enable_replication(self) -> tuple[str, Dict[str, Any]]:
        sql_str = f"ALTER DATABASE {self._db_name} ENABLE REPLICATION TO ACCOUNTS "
        sql_str += ", ".join(self._prop["accounts"])
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def disable_replication(self) -> tuple[str, Dict[str, Any]]:
        sql_str = f"ALTER DATABASE {self._db_name} DISABLE REPLICATION "
        account_identifiers = self._prop["accounts"]
        if account_identifiers:
            sql_str += "TO ACCOUNTS "
            sql_str += ", ".join(account_identifiers)
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def refresh_replication(self) -> tuple[str, Dict[str, Any]]:
        sql_str = f"ALTER DATABASE {self._db_name} REFRESH"
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def enable_failover(self) -> tuple[str, Dict[str, Any]]:
        sql_str = f"ALTER DATABASE {self._db_name} ENABLE FAILOVER TO ACCOUNTS "
        sql_str += ", ".join(self._prop["accounts"])
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def disable_failover(self) -> tuple[str, Dict[str, Any]]:
        sql_str = f"ALTER DATABASE {self._db_name} DISABLE FAILOVER "
        account_identifiers = self._prop["accounts"]
        if account_identifiers:
            sql_str += "TO ACCOUNTS "
            sql_str += ", ".join(account_identifiers)
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def primary_failover(self) -> tuple[str, Dict[str, Any]]:
        sql_str = f"ALTER DATABASE {self._db_name} PRIMARY"
        return sql_str, self.snow_exec.execute(sql_str)[0]

    @staticmethod
    def _transform_show_response(resp: Dict[str, Any]) -> Dict[str, Any]:
        for k in (
            "is_default",
            "is_current",
        ):
            resp[k] = (resp[k].upper() == "Y")
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
        return list(map(DatabaseResource._transform_show_response, resp))
