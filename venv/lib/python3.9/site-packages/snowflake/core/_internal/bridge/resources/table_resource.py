# Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
import re

from contextlib import suppress
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import unquote

from snowflake.connector import SnowflakeConnection
from snowflake.snowpark._internal.utils import is_single_quoted

from ...utils import _retrieve_parameter_value, double_quote_name, normalize_name, try_single_quote_value, unquote_name
from ..rest_errors import BadRequest, InternalServerError, NotFound, RestError
from ..snow_request import SnowRequest
from .resource_base import ResourceBase


class TableResource(ResourceBase):
    resource_name = "tables"
    table_components = [
        "api",
        "v2",
        "databases",
        "database_name",
        "schemas",
        "schema_name",
        "tables",
        "table_name"
    ]

    def __init__(self, req: SnowRequest, conn_ob: SnowflakeConnection):
        super().__init__(conn_ob)
        self._database = ""
        self._dependents = False
        self._is_create_or_show = False
        self._method = req.method
        self._parameter_key = ""
        self._prop = req.body if req.body is not None else dict()
        self._query_params = req.query_params
        self._schema = ""
        self._full_table_name = ""
        self._table_name = ""

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
        self._is_create_or_show = False
        self._database = normalize_name(
            unquote(path_parts[TableResource.table_components.index("database_name")])
        )
        self._schema = normalize_name(
            unquote(path_parts[TableResource.table_components.index("schema_name")])
        )
        index_of_table_name_in_url = TableResource.table_components.index("table_name")

        if len(path_parts) < TableResource.table_components.index("tables") + 1\
            or len(path_parts) > index_of_table_name_in_url + 1:
            raise BadRequest("Malformed Resource URL")

        path_parts_contains_table_name = len(path_parts) >= index_of_table_name_in_url + 1
        if path_parts_contains_table_name:
            table_name = path_parts[index_of_table_name_in_url]
            self._table_name = normalize_name(
                unquote(table_name)
            )
            self._full_table_name = f"{self._database}.{self._schema}.{self._table_name}"

        self._is_create_or_show = (not path_parts_contains_table_name) or (
            self._action in ('create_like', 'using_template', 'as_select')
        )

    def execute(self) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Routing function

        Using the VERB from the Rest request and the
        metadata collected from the call route to the
        appropriate SQL translation function.

        Each function returns the SQL string and the result
        of the execution result The former is to allow
        testability of individual functions.
        """
        ret: Union[List[Dict[str, Any]], Dict[str, Any]] = [{}]
        if self._method == "PUT":
            if self._full_table_name != "":
                _, ret = self.create_or_alter_table()
        elif self._method == "GET":
            if self._is_create_or_show:
                _, ret = self.show_tables()
            elif self._full_table_name != "":
                _, ret = self.desc_table()
        elif self._method == "POST":
            if self._action == 'clone':
                _, ret = self.clone_table()
            elif self._is_create_or_show:
                _, ret = self.create_table()
            elif self._action != "":
                if self._action == "undelete":
                    _, ret = self.undelete_table()
                elif self._action == "suspend_recluster":
                    _, ret = self.suspend_recluster()
                elif self._action == "resume_recluster":
                    _, ret = self.resume_recluster()
                elif self._action == "swapwith":
                    _, ret = self.swap_with()
            else:
                raise BadRequest("Bad Resource Hierarchy")
        elif self._method == "DELETE":
            if self._full_table_name != "" and self._parameter_key == "":
                _, ret = self.delete_table()
            else:
                raise BadRequest("Incorrect Path")
        else:
            raise BadRequest("REST VERB not supported")

        return ret

    def desc_table(self) -> Tuple[str, Dict[str, Any]]:
        return self._desc_table()

    def _desc_table(self) -> Tuple[str, Dict[str, Any]]:
        sql_str = f"SHOW tables like '{unquote_name(self._table_name)}' in schema {self._database}.{self._schema}"
        tables = self.snow_exec.execute(sql_str)
        if not tables:
            raise NotFound(f"Table {self._table_name} doesn't exist.")
        for table in tables:
            if normalize_name(table["name"]) == self._table_name:
                self._validate_table([table], True)
                return sql_str, tables[0]
        raise NotFound(f"Table {self._table_name} doesn't exist.")

    def show_tables(self) -> Tuple[str, List[Dict[str, Any]]]:
        deep: bool = self._query_params["deep"]  # type: ignore
        sql_str = "SHOW "
        if not deep:
            sql_str += "TERSE "

        if self._query_params.get("history") is True:  # type: ignore
            sql_str += "HISTORY "

        sql_str += "TABLES "

        if "like" in self._query_params:
            sql_str += " LIKE '{}' ".format(self._query_params["like"])

        sql_str += f"in SCHEMA {self._database}.{self._schema} "
        # TODO: This limits the search to a specific schema.
        #  Harsh is going to discuss with REST team how to allow users to search tables and other entities without
        #  the scope of database and/or schema.

        if "startsWith" in self._query_params:
            sql_str += "STARTS WITH {} ".format(self._query_params["startsWith"])

        if "rootOnly" in self._query_params and self._query_params["rootOnly"]:
            sql_str += "ROOT ONLY "

        if "showLimit" in self._query_params:
            sql_str += "LIMIT {} ".format(self._query_params["showLimit"])
            if "fromName" in self._query_params:
                sql_str += "FROM {} ".format(self._query_params["fromName"])

        tables = self.snow_exec.execute(sql_str)
        tables = [x for x in tables if
                  not x.get("dropped_on") and
                  (x.get("is_event") == 'N' or not x.get("is_event")) and
                  (x.get("is_external") == 'N') or not x.get("is_external") and
                  (x.get("is_hybrid") == 'N') or not x.get("is_hybrid")]
        self._validate_table(tables, deep=deep)

        return sql_str, tables

    def delete_table(self) -> Tuple[str, Dict[str, Any]]:
        if "ifExists" in self._query_params and self._query_params["ifExists"]:
            sql_str = f"DROP table IF EXISTS {self._full_table_name}"
        else:
            sql_str = f"DROP table {self._full_table_name}"
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def undelete_table(self) -> Tuple[str, Dict[str, Any]]:
        sql_str = f"UNDROP table {self._full_table_name}"
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def suspend_recluster(self) -> Tuple[str, Dict[str, Any]]:
        sql_str = f"ALTER table {self._full_table_name} suspend recluster "
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def resume_recluster(self) -> Tuple[str, Dict[str, Any]]:
        sql_str = f"ALTER table {self._full_table_name} resume recluster "
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def swap_with(self) -> Tuple[str, Dict[str, Any]]:
        sql_str = f"ALTER table {self._full_table_name} SWAP WITH {self._query_params['targetTableName']}"
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def create_or_alter_table(self) -> Tuple[str, Dict[str, Any]]:
        """
        columns = t.get("columns")
        constraints = t.get("constraints")
        cluster_by = t.get("cluster_by")
        enable_schema_evolution: bool = t.get("enable_schema_evolution")  # type: ignore
        stage_file_format_name = t.get("stage_file_format_name")
        stage_file_format_type = t.get("stage_file_format_type")
        stage_file_format_type_options = t.get("stage_file_format_type_options")
        stage_copy_options = t.get("stage_copy_options")
        data_retention_time_in_days = t.get("data_retention_time_in_days")
        max_data_extension_time_in_days = t.get("max_data_extension_time_in_days")
        change_tracking = t.get("change_tracking")
        default_ddl_collation = t.get("default_ddl_collation")
        comment = t.get("comment")
        """
        if normalize_name(self._table_name) != normalize_name(self._prop["name"]):
            raise BadRequest(
                f"Table name {self._table_name} in path doesn't match input Table object's name {self._prop['name']}")
        try:
            _, original = self._desc_table()
        except NotFound:
            return self.create_table()
        kind = self._prop.get("kind", "TABLE")
        if kind:
            kind = kind.lower()
        original_kind = original["kind"].lower()
        if kind != original_kind.lower() and not (kind == "temp" and original_kind == "temporary"):
            raise BadRequest(f"Table kind must match. Original kind is {original_kind} while new kind is {kind}")
        full_table_name = f"{double_quote_name(self._database)}.{double_quote_name(self._schema)}.{double_quote_name(self._table_name)}"
        new = self._prop
        sqls = []
        # handle properties and object parameters
        set_new_value_properties = []
        unset_value_properties = []
        for property_name in (
            "data_retention_time_in_days",
            "enable_schema_evolution",
            "max_data_extension_time_in_days",
            "change_tracking",
            "default_ddl_collation",
            "comment"
        ):
            property_new_value = new.get(property_name)
            if property_name == "default_ddl_collation" and property_new_value:
                property_new_value = property_new_value.upper()
            if property_new_value != original.get(property_name):
                if property_new_value is None:
                    unset_value_properties.append(property_name)
                else:
                    set_new_value_properties.append(property_name)
        if set_new_value_properties:
            set_property_sql = " ".join([f"{x}={try_single_quote_value(new.get(x))}" for x in set_new_value_properties])
            sqls.append(f"alter table {full_table_name} set {set_property_sql}")
        for property_name in unset_value_properties:
            sqls.append(f"alter table {full_table_name} unset {property_name}")
        original_columns = original["columns"]
        new_columns = new.get("columns")
        if not new_columns:
            raise BadRequest("Columns must be provided for create_or_update")

        def compare_column(old_column: Dict[str, Any], new_column: Dict[str, Any]) -> str:
            column_name = new_column["name"]
            normalized_column_name = normalize_name(column_name)
            if normalized_column_name != normalize_name(old_column["name"]):
                raise BadRequest(f"Can't remove a column for create_or_update. {new_column['name']} is removed.")
            column_sqls = []
            old_datatype = normalize_datatype(old_column["datatype"])
            new_datatype = normalize_datatype(new_column["datatype"])
            if old_datatype != new_datatype:
                column_sqls.append(f"{normalized_column_name} {new_datatype} ")
            if old_column.get("default") != new_column.get("default"):
                if new_column.get("default") is None:
                    column_sqls.append(f" {normalized_column_name} drop default")
                else:
                    column_sqls.append(f" {normalized_column_name} set default {new_column['default']}")
            if old_column.get("nullable") != new_column.get("nullable"):
                if new_column.get("nullable") is True:
                    column_sqls.append(f" {normalized_column_name} drop not null")
                else:
                    column_sqls.append(f" {normalized_column_name} not null")
            if old_column.get("comment") != new_column.get("comment"):
                if new_column["comment"]:
                    column_sqls.append(f""" {normalized_column_name} comment '{new_column["comment"]}'""")
                else:
                    column_sqls.append(f" {normalized_column_name} unset comment")
            new_collate = new_column.get("collate")
            if new_collate and old_column.get("collate", "").lower() != new_collate.lower():
                raise BadRequest("Can't update a column collate.")

            new_identity = bool(new_column.get("autoincrement"))
            old_identity = bool(old_column.get("autoincrement"))
            if new_identity != old_identity:
                raise BadRequest(f"'autoincrement' of Column {column_name} can't be updated.")
            for prop in ("autoincrement_start", "autoincrement_increment"):
                if old_column.get(prop) != new_column.get(prop):
                    raise BadRequest(f"'{prop}' of Column {column_name} can't be updated.")
            return ",".join(column_sqls)

        # process columns
        length = min(len(new_columns), len(original_columns))
        for i in range(length):
            alter_column_sql = compare_column(original_columns[i], new_columns[i])
            if alter_column_sql:
                sqls.append(f"alter table {full_table_name} modify {alter_column_sql}")
        if len(original_columns) > len(new_columns):
            raise BadRequest(
                f"""Can't remove a column for create_or_update. These columns are removed {",".join([original_columns[x]["name"] for x in range(length, len(original_columns))])}.""")
        for j in range(length, len(new_columns)):
            sqls.append(f"alter table {full_table_name} add column {column_to_sql(new_columns[j])}")

        # process constraints
        # consolidate new outofline and inline constraints
        new_constraints = []
        for col_name, col_constraints in [(c["name"], c["constraints"]) for c in new_columns if c.get("constraints") is not None]:
            for col_con in col_constraints:
                col_con["column_names"] = [col_name]
                new_constraints.append(col_con)
        outofline_constraints = new.get("constraints")
        if outofline_constraints:
            new_constraints.extend(outofline_constraints)
        # end of consolidate new outofline and inline constraints

        original_constraints = original.get("constraints") or []
        original_pks = [c for c in original_constraints if c.get("constraint_type") == "PRIMARY KEY"]
        original_pk = original_pks[0] if original_pks else None  # A table has only one PK
        original_uks = {tuple(normalize_name(x) for x in c.get("column_names")): c for c in original_constraints if
                        c.get("constraint_type") == "UNIQUE"}
        # TODO: There is no SQL to retrieve a foreign key's referencing columns.
        #  So create_or_alter doesn't support foreign key yet. Pls don't remove the commented code below.
        # original_fks = {foreign_key_to_tuple(c): c for c in original_constraints if
        #                 c.get("constraint_type") == "FOREIGN KEY"}

        pks = [c for c in new_constraints if c.get("constraint_type") == "PRIMARY KEY"]
        if len(pks) > 1:
            raise BadRequest("There should be only one primary key defined.")
        if len(pks) == 1:
            pk = pks[0]
            if original_pk:
                original_pk_name = normalize_name(original_pk.get("name"))
                pk_name = normalize_name(pk.get("name")) if pk.get("name") else None
                if tuple(normalize_name(x) for x in pk.get("column_names")) == tuple(normalize_name(x) for x in original_pk.get("column_names")):
                    if pk_name == original_pk_name or (pk_name is None and match_sys_name(original_pk_name)):
                        pass
                    elif pk_name:
                        sqls.append(f"alter table {full_table_name} rename constraint {original_pk_name} to {pk_name}")
                    else:
                        sqls.append(f"alter table {full_table_name} drop constraint {original_pk_name}")
                        sqls.append(f"alter table {full_table_name} add {constraint_to_sql(pk)} ")
                else:
                    sqls.append(f"alter table {full_table_name} drop constraint {original_pk_name}")
                    sqls.append(f"alter table {full_table_name} add {constraint_to_sql(pk)} ")
            else:
                sqls.append(f"alter table {full_table_name} add {constraint_to_sql(pk)} ")
        elif original_pk:
            original_pk_name = normalize_name(original_pk.get("name"))
            sqls.append(f"alter table {full_table_name} drop constraint {original_pk_name}")

        uks = {tuple(normalize_name(x) for x in c.get("column_names")): c for c in new_constraints if c.get("constraint_type") == "UNIQUE"}
        for original_uk_columns, original_con in original_uks.items():
            original_con_name = normalize_name(original_con.get("name"))
            if original_uk_columns not in uks:
                sqls.append(f"alter table {full_table_name} drop constraint {original_con_name}")

        for uk_columns, con in uks.items():
            con_name = normalize_name(con.get("name")) if con.get("name") else None
            if original_uks and uk_columns in original_uks:
                original_con_name = normalize_name(original_uks[uk_columns].get("name"))
                if con_name is None and match_sys_name(original_con_name):
                    continue
                if con_name != original_con_name:
                    sqls.append(f"alter table {full_table_name} rename constraint {original_con_name} to {con_name}")
            else:
                sqls.append(f"alter table {full_table_name} add {constraint_to_sql(con)}")

        # TODO: There is no SQL to retrieve a foreign key's referencing columns.
        #  So create_or_alter doesn't support foreign key yet. Pls don't remove the commented code below.
        # fks = {foreign_key_to_tuple(c): c for c in new_constraints if
        #        c.get("constraint_type") == "FOREIGN KEY"}
        # for original_fk_tuple, original_con in original_fks.items():
        #     original_con_name = normalize_name(original_con.get("name"))
        #     if original_fk_tuple not in fks:
        #         sqls.append(f"alter table {full_table_name} drop constraint {original_con_name}")
        #
        # for fk_tuple, con in fks.items():
        #     con_name = con.get("name")
        #     if original_uks and fk_tuple in original_uks:
        #         original_con_name = normalize_name(original_uks[fk_tuple].get("name"))
        #         if con_name is None and match_sys_name(original_con_name):
        #             continue
        #         if con_name != original_con_name:
        #             sqls.append(f"alter table {full_table_name} rename constraint {original_con_name} to {con_name}")
        #     else:
        #         sqls.append(f"alter table {full_table_name} add {constraint_to_sql(con)}")

        # process cluster by
        new_cluster_by = new.get("cluster_by")
        if original.get("cluster_by") != new_cluster_by:
            if new_cluster_by:
                sqls.append(f"alter table {full_table_name} cluster by ({','.join(new_cluster_by)})")
            else:
                sqls.append(f"alter table {full_table_name} drop clustering key")

        # end of processing
        try:
            for query in sqls:
                self.snow_exec.execute(query)
        except RestError as re:
            raise InternalServerError(
                "Could not successfully put the table on Snowflake. "
                "Kindly check for data inconsistencies"
            ) from re

        # We send one success message for the entire batch of SQL statements.
        return str(sqls), {"description": "successful"}

    def get_new_table_name(self) -> str:
        if self._action in ('using_template', 'as_select'):
            table_name = self._table_name
        elif self._action == 'create_like':
            table_name = self._query_params["newTableName"]
        else:
            table_name = self._prop['name']

        _, _, table_name = table_name.rpartition('.')
        return table_name

    def table_creation_prefix(self) -> str:
        kind = self._prop.get("kind", "")
        if kind and kind.lower().strip(" ") == "table":
            kind = ""
        table_start_sql = f'{kind}  table {self._database}.{self._schema}.{normalize_name(self.get_new_table_name())}'

        sql_str = "CREATE "
        if "createMode" in self._query_params:
            createMode = self._query_params["createMode"]
            # No Op for errorIfExists as that is default behavior
            if createMode == "orReplace":
                sql_str += f"OR REPLACE {table_start_sql} "
            elif createMode == "ifNotExists":
                sql_str += f"{table_start_sql} IF NOT EXISTS "
            elif createMode == "errorIfExists" or not createMode:
                sql_str += table_start_sql
            else:
                raise BadRequest(
                    "Unsupported createMode mentioned {}".format(
                        self._query_params["createMode"]
                    )
                )
        else:
            sql_str += table_start_sql
        sql_str += " "
        return sql_str

    def clone_table(self) -> Tuple[str, Dict[str, Any]]:
        sql_str = self.table_creation_prefix()
        if "name" not in self._prop:
            raise BadRequest("Name is a required field for cloning a table")

        clone: Union[str, None] = self._prop.get("name")
        if clone is not None:
            sql_str += f"CLONE {normalize_name(self._table_name)} "
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

    def create_table(self) -> Tuple[str, Dict[str, Any]]:
        sql_str = self.table_creation_prefix()

        create_table_query = self._get_create_table_query()

        copy_grants = self._query_params.get("copy_grants")
        copy_grants = " copy grants" if copy_grants else ""

        # TODO: Add policy, tags, search_optimization_targets
        sql_str = f"{sql_str} {table_to_sql(self._prop)}{copy_grants}{create_table_query}"
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def _get_create_table_query(self) -> str:
        query = ""
        if self._action == 'clone':
            query += f' clone {self._prop}'
        elif self._action == 'create_like':
            query += f' like {self._full_table_name}'
        elif self._action == 'as_select':
            query += f' as {self._query_params.get("query")}'
        elif self._action == 'using_template':
            query += f' using template ({self._query_params.get("query")})'

        return query

    def _validate_table(self, tables: List[Dict[str, Any]], deep: bool = False) -> List[Dict[str, Any]]:
        for t in tables:
            t["search_optimization"] = True if t.get("search_optimization") == "ON" else False
            t["change_tracking"] = True if t.get("change_tracking") == "ON" else False
            t["automatic_clustering"] = True if t.get("automatic_clustering") == "ON" else False
            t["enable_schema_evolution"] = True if t.get("enable_schema_evolution") == "Y" else False
            retention_time = t.get("retention_time")
            if retention_time:
                t["data_retention_time_in_days"] = int(retention_time)
            cluster_by = t.get("cluster_by")
            if cluster_by and cluster_by.startswith("LINEAR"):
                cluster_by = cluster_by[7:-1]  # remove LINEAR( and ) to retrieve the value
            t["cluster_by"] = cluster_by.split(",") if cluster_by else None
            full_table_name = f"{double_quote_name(t['database_name'])}.{double_quote_name(t['schema_name'])}.{double_quote_name(t['name'])}"
            object_parameters = self._fetch_object_parameters(full_table_name)
            t["data_retention_time_in_days"] = object_parameters.get("DATA_RETENTION_TIME_IN_DAYS")
            t["max_data_extension_time_in_days"] = object_parameters.get("MAX_DATA_EXTENSION_TIME_IN_DAYS")
            t["default_ddl_collation"] = object_parameters.get("DEFAULT_DDL_COLLATION")

            delete_dict_key(t, "retention_time")
            delete_dict_key(t, "is_external")
            delete_dict_key(t, "is_event")
            delete_dict_key(t, "is_hybrid")
            if deep:
                t["columns"] = self._fetch_columns(t["name"])
                t["constraints"] = self._fetch_constraints(t["name"])
            if t.get("comment") == "":
                t["comment"] = None
        return tables

    def _fetch_object_parameters(self, table_name: str) -> Dict[str, Any]:
        sql = f"show parameters in table {table_name}"
        parameters = self.snow_exec.execute(sql)
        d = {}
        for table_level_parameter in [p for p in parameters if p["level"] == "TABLE"]:
            d[table_level_parameter["key"]] = _retrieve_parameter_value(table_level_parameter)
        return d

    def _fetch_columns(self, table_name: str) -> List[Dict[str, Any]]:
        sql = f"select COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLLATION_NAME, COLUMN_DEFAULT, " \
              f"IS_IDENTITY, IDENTITY_START, IDENTITY_INCREMENT, COMMENT " \
              f"from {self._database}.information_schema.columns " \
              f"where table_catalog='{self._database}' " \
              f"and table_schema='{self._schema}' and table_name= '{table_name}' order by ORDINAL_POSITION"
        # inline constraints can't be obtained.
        columns_data = self.snow_exec.execute(sql)
        columns = []
        for item in columns_data:
            column = {
                "name": item["COLUMN_NAME"],
                "datatype": item["DATA_TYPE"],
                "nullable": item["IS_NULLABLE"] == "YES",
                "collate": item["COLLATION_NAME"],
                "default": item["COLUMN_DEFAULT"],
                "autoincrement": item["IS_IDENTITY"] == "YES",
                "autoincrement_start": item["IDENTITY_START"],
                "autoincrement_increment": item["IDENTITY_INCREMENT"],
                "comment": item["COMMENT"],
            }
            columns.append(column)
        return columns

    def _fetch_constraints(self, table_name: str) -> List[Dict[str, Any]]:
        result = []
        constraints = self.snow_exec.execute(
            f"select CONSTRAINT_NAME, CONSTRAINT_TYPE from {self._database}.information_schema.table_constraints where CONSTRAINT_CATALOG='{self._database}' and "
            f"CONSTRAINT_SCHEMA = '{self._schema}' and TABLE_NAME = '{table_name}' order by CONSTRAINT_NAME"
        )
        primary_keys = self.snow_exec.execute(
            f"show primary keys in table {self._database}.{self._schema}.{double_quote_name(table_name)}"
        )
        unique_keys = self.snow_exec.execute(
            f"show unique keys in table {self._database}.{self._schema}.{double_quote_name(table_name)}"
        )
        for constraint in constraints:
            constraint_type = constraint["CONSTRAINT_TYPE"]
            constraint_name = constraint["CONSTRAINT_NAME"]
            result_con = {"name": constraint_name}
            if constraint_type == "UNIQUE":
                constraint_columns = [uk for uk in unique_keys if uk["constraint_name"] == constraint_name]
                constraint_columns = [
                    con["column_name"] for con in sorted(constraint_columns, key=lambda x: x["key_sequence"])
                ]
                result_con["column_names"] = constraint_columns
                result_con["constraint_type"] = "UNIQUE"
            elif constraint_type == "PRIMARY KEY":
                constraint_columns = [pk for pk in primary_keys if pk["constraint_name"] == constraint_name]
                constraint_columns = [
                    con["column_name"] for con in sorted(constraint_columns, key=lambda x: x["key_sequence"])
                ]
                result_con["column_names"] = constraint_columns
                result_con["constraint_type"] = "PRIMARY KEY"
            elif constraint_type == "FOREIGN KEY":
                raise NotImplementedError("constraint type FOREIGN KEY isn't not yet supported in create_or_update table.")
                # referenced_constraint = self.snow_exec.execute(
                #     f"select CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME, UNIQUE_CONSTRAINT_SCHEMA from information_schema.referential_constraints where CONSTRAINT_CATALOG='{self._database}' and "
                #     f"CONSTRAINT_SCHEMA = '{self._schema}' and CONSTRAINT_NAME = '{constraint_name}'"
                # )[0]
                # referenced_key = self.snow_exec.execute(
                #     f"""show primary keys like '{referenced_constraint["UNIQUE_CONSTRAINT_NAME"]}' in schema {referenced_constraint["UNIQUE_CONSTRAINT_CATALOG"]}.{referenced_constraint["UNIQUE_CONSTRAINT_SCHEMA"]}"""
                # )
                # if not referenced_key:
                #     referenced_key = self.snow_exec.execute(
                #         f"""show unique keys like '{referenced_constraint["UNIQUE_CONSTRAINT_NAME"]}' in schema {referenced_constraint["UNIQUE_CONSTRAINT_CATALOG"]}.{referenced_constraint["UNIQUE_CONSTRAINT_SCHEMA"]}"""
                #     )
                # referenced_table = referenced_key[0]["TABLE_NAME"]
                # result_con["column_names"] = ...  # there isn't a way to ge this list.
                # result_con["referenced_table_name"] = referenced_table
                # result_con["referenced_column_names"] = [
                #     con["column_name"] for con in sorted(referenced_key, key=lambda x: x["key_sequence"])]
                # result_con["constraint_type"] = "FOREIGN KEY"
            else:
                raise ValueError(f"Unknown constraint type {constraint_type}")
            result.append(result_con)
        return result


def constraint_to_sql(c: Dict[str, Any]) -> str:
    constraint_type = c.get("constraint_type")
    constraint_name = c.get("name")
    constraint_name_sql = ""
    if constraint_name:
        constraint_name_sql = f" constraint {normalize_name(constraint_name)}"
    if constraint_type == "PRIMARY KEY":
        return constraint_name_sql + primary_key_to_sql(c)
    # TODO: There is no SQL to retrieve a foreign key's referencing columns.
    #  So create_or_alter doesn't support foreign key yet. Pls don't remove the commented code below.
    # elif constraint_type == "FOREIGN KEY":
    #     return constraint_name_sql + foreign_key_to_sql(c)
    elif constraint_type == "UNIQUE":
        return constraint_name_sql + unique_key_to_sql(c)
    raise BadRequest(f"Wrong constraint type '{constraint_type}'")


def primary_key_to_sql(pk: Dict[str, Any]) -> str:
    column_names = pk.get("column_names")
    columns = [normalize_name(x) for x in column_names] if column_names else None
    if columns:
        return f" primary key ({','.join(columns)})"
    return ""


def unique_key_to_sql(pk: Dict[str, Any]) -> str:
    column_names = pk.get("column_names")
    columns = [normalize_name(x) for x in column_names] if column_names else None
    if columns:
        return f" unique ({','.join(columns)})"
    return ""


# TODO: There is no SQL to retrieve a foreign key's referencing columns.
#  So create_or_alter doesn't support foreign key yet. Pls don't remove the commented code below.

# def foreign_key_to_sql(f: Dict[str, Any]) -> str:
#     column_names = [normalize_name(x) for x in f.get("column_names")]
#     referenced_table_name = normalize_name(f.get("table_name"))
#     referenced_column_names = [normalize_name(x) for x in f.get("referenced_column_names")] if f.get("referenced_column_names") else None
#
#     column_names_sql = f"({','.join(column_names)})" if column_names else ""
#     referenced_column_names_sql = f"({','.join(referenced_column_names)})" if referenced_column_names else ""
#     s = f" foreign key references {column_names_sql} {referenced_table_name} {referenced_column_names_sql}"
#     return s


def column_to_sql(c: Dict[str, Any]) -> str:
    name = c["name"]
    datatype = c.get("datatype")
    nullable = c.get("nullable")
    collate = c.get("collate")
    default = c.get("default")
    autoincrement = c.get("autoincrement")
    autoincrement_start = c.get("autoincrement_start")
    autoincrement_increment = c.get("autoincrement_increment")
    constraint = c.get("constraint")
    comment = c.get("comment")
    cs = f"""{normalize_name(name)} {datatype}\
{' not null' if not nullable else ''}\
{name_value_to_sql("collate", collate, False, True)}\
{' default' if default is not None else ""} {default if default is not None else ""}\
{" autoincrement" if autoincrement else ""} {f"start {autoincrement_start}" if autoincrement_start is not None else ""} {f"increment {autoincrement_increment}" if autoincrement_increment else ""}\
{constraint_to_sql(constraint) if constraint is not None else ""}\
{f" comment '{comment}'" if comment else ""}\
"""
    return cs


def table_to_sql(t: Dict[str, Any]) -> str:
    # kind = t.get("kind")
    # name = t.get("name")
    columns = t.get("columns")
    constraints = t.get("constraints")
    cluster_by = t.get("cluster_by")
    enable_schema_evolution: bool = t.get("enable_schema_evolution")  # type: ignore
    stage_file_format_name = t.get("stage_file_format_name")
    stage_file_format_type = t.get("stage_file_format_type")
    stage_file_format_type_options = t.get("stage_file_format_type_options")
    stage_copy_options = t.get("stage_copy_options")
    data_retention_time_in_days = t.get("data_retention_time_in_days")
    max_data_extension_time_in_days = t.get("max_data_extension_time_in_days")
    change_tracking = t.get("change_tracking")
    default_ddl_collation = t.get("default_ddl_collation")
    comment = t.get("comment")
    constraint_sql = ",".join([constraint_to_sql(c) for c in constraints]) if constraints else ""
    columns_sql = f"""({",".join([column_to_sql(c) for c in columns]) if columns else ""} \
{"," if constraint_sql else ""}{constraint_sql})"""

    ts = f"""{columns_sql} \
{"cluster by(" + ",".join(cluster_by) if cluster_by else ""}{")" if cluster_by else ""} \
{name_value_to_sql("enable_schema_evolution", enable_schema_evolution, True)} \
{stage_file_format_to_sql(stage_file_format_name, stage_file_format_type, stage_file_format_type_options)}
{get_options_statement("stage_copy_options", stage_copy_options)} \
{name_value_to_sql("data_retention_time_in_days", data_retention_time_in_days, True)} \
{name_value_to_sql("max_data_extension_time_in_days", max_data_extension_time_in_days, True)} \
{name_value_to_sql("change_tracking", change_tracking, True)} \
{name_value_to_sql("default_ddl_collation", default_ddl_collation, True)} \
{name_value_to_sql("comment", comment, True, True)}
    """ if columns or constraints else ""
    return ts


# TODO: There is no SQL to retrieve a foreign key's referencing columns.
#  So create_or_alter doesn't support foreign key yet. Pls don't remove the commented code below.

# def foreign_key_to_tuple(fk: Dict[str, Any]) -> Tuple[Tuple[str, ...], ...]:
#     return tuple(normalize_name(x) for x in fk["referencing_column_names"]), (normalize_name(fk["referenced_table_name"]),), tuple(normalize_name(x) for x in fk["referenced_column_names"]) if fk.get("referenced_column_names") else ("",)


def name_value_to_sql(prop_name: str, value: Any, use_equal: bool = False, quote_value: bool = False) -> str:
    equal = "=" if use_equal else " "
    value = try_single_quote_value(value) if quote_value else value
    return "" if not value else f" {prop_name}{equal}{value}"


def bool_value_to_sql(prop_name: str, value: bool) -> str:
    return prop_name if value else ""


def dict_value_to_sql(value: Optional[Union[str, bool, int, float]]) -> str:
    if value is None:
        return ""

    if isinstance(value, str):
        if len(value) > 1 and is_single_quoted(value):
            return value
        else:
            value = value.replace(
                "'", "''"
            )  # escape single quotes before adding a pair of quotes
            return f"'{value}'"
    else:
        return str(value)


def get_options_statement(prop_name: str, options: Optional[Dict[str, Any]]) -> str:
    return ""
    # if not options:
    #     return ""
    # return f"""{prop_name} {"=" if prop_name else ''} ({" ".join(f"{k}={dict_value_to_sql(v)}" for k, v in options.items() if v is not None)}) """


def stage_file_format_to_sql(stage_file_format_name: Optional[str] = None, stage_file_format_type: Optional[str] = None,
                             stage_file_format_type_options: Optional[
                                 Dict[str, Union[str, bool, int, float]]] = None) -> str:
    return ""
    # if not stage_file_format_name and not stage_file_format_type:
    #     return ""
    # format_name_sql = f"format_name={stage_file_format_name}" if stage_file_format_name else ""
    # format_type_sql = f"type={stage_file_format_type} {get_options_statement('', stage_file_format_type_options)}" if stage_file_format_type else ""
    # format_sql = format_name_sql or format_type_sql
    # sql = f"STAGE_FILE_FORMAT=({format_sql})" if format_sql else ""
    # return sql


SYSTEM_NAME_PATTERN = re.compile(r'"SYS_CONSTRAINT_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"')


def match_sys_name(name: Optional[str]) -> bool:
    if not name:
        return True
    match = SYSTEM_NAME_PATTERN.match(name)
    if match:
        return True
    return False


def normalize_datatype(datatype: str) -> str:
    """Convert equivalent datatypes to the same one according to
    https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
    All string types to varchar, and all numeric types to number and float.
    """
    datatype = datatype.upper()
    datatype = datatype.replace(" ", "")
    if datatype in ("INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT", "NUMBER"):
        return "NUMBER(38,0)"
    if datatype in ("DOUBLE", "DOUBLEPRECISION", "REAL"):
        return "FLOAT"
    if datatype in ("STRING", "TEXT", "VARCHAR"):
        return "VARCHAR(16777216)"
    if datatype in ("CHAR", "CHARACTER"):
        return "VARCHAR(1)"
    if datatype in ("VARBINARY"):
        return "BINARY"
    datatype = datatype.replace("DECIMAL", "NUMBER").replace("NUMERIC", "NUMBER")\
        .replace("STRING", "VARCHAR").replace("TEXT", "VARCHAR")
    return datatype


def delete_dict_key(d: Dict[str, Any], key: str) -> None:
    with suppress(KeyError):
        del d[key]
