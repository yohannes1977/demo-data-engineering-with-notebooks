# Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
import json

from typing import Any, Dict, List, Tuple, Union
from urllib.parse import unquote

from snowflake.connector import SnowflakeConnection
from snowflake.snowpark._internal.utils import is_single_quoted, parse_table_name

from ...utils import normalize_name, try_single_quote_value, unquote_name
from ..rest_errors import BadRequest, InternalServerError, NotFound, RestError
from ..snow_request import SnowRequest
from .resource_base import ResourceBase


class TaskResource(ResourceBase):
    task_prop_list = {
        "required": ["name", "definition"],
        "optional": [
            "warehouse",
            "schedule",
            "comment",
            "config",
            "session_parameters",
            "predecessors",
            "user_task_managed_initial_warehouse_size",
            "user_task_timeout_ms",
            "condition",
            "allow_overlapping_execution",
            "error_integration",
            "suspend_task_after_num_failures",
        ],
    }
    resource_name = "tasks"
    task_components = [
        "api",
        "v2",
        "databases",
        "database_name",
        "schemas",
        "schema_name",
        "tasks",
        "task_name",
        "task_sub_resource",
    ]

    def __init__(self, req: SnowRequest, conn_ob: SnowflakeConnection):
        super().__init__(conn_ob)
        self._complete_graphs = False
        self._current_graphs = False
        self._database = ""
        self._dependents = False
        self._is_collection = False
        self._method = req.method
        self._prop = req.body if req.body is not None else dict()
        self._query_params = req.query_params
        self._schema = ""
        self._task_name = ""
        self._task_original_name = ""

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
        self._database = normalize_name(
            unquote(path_parts[TaskResource.task_components.index("database_name")])
        )
        self._schema = normalize_name(
            unquote(path_parts[TaskResource.task_components.index("schema_name")])
        )
        if len(path_parts) < TaskResource.task_components.index("tasks") + 1:
            raise BadRequest("Malformed Resource URL")  # pragma: no cover
        elif len(path_parts) >= TaskResource.task_components.index("task_name") + 1:
            self._task_original_name = normalize_name(
                unquote(path_parts[TaskResource.task_components.index("task_name")])
            )
            self._task_name = f"{self._database}.{self._schema}.{self._task_original_name}"
            if len(path_parts) > TaskResource.task_components.index("task_name") + 1:
                task_sub_resource = path_parts[
                    TaskResource.task_components.index("task_sub_resource")
                ]
                if task_sub_resource == "dependents":
                    self._dependents = True
                elif task_sub_resource == "current_graphs":
                    self._current_graphs = True
                elif task_sub_resource == "complete_graphs":
                    self._complete_graphs = True
        elif len(path_parts) == TaskResource.task_components.index("tasks") + 1:
            self._is_collection = True

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
            if self._task_name != "":
                _, ret = self._create_or_alter_task()
        elif self._method == "GET":
            if self._is_collection:
                _, ret = self._show_tasks()
            elif self._dependents:
                _, ret = self._get_dependents()
            elif self._complete_graphs:
                _, ret = self._get_complete_graphs()
            elif self._current_graphs:
                _, ret = self._get_current_graphs()
            elif self._task_name != "":
                _, ret = self._desc_task()
        elif self._method == "POST":
            if self._is_collection:
                _, ret = self._create_task()
            elif self._action != "":
                if self._action.endswith("suspend"):
                    _, ret = self._suspend_task()
                elif self._action.endswith("resume"):
                    _, ret = self._resume_task()
                elif self._action.endswith("execute"):
                    _, ret = self._execute_task()
            else:
                raise BadRequest("Bad Resource Hierarchy")  # pragma: no cover
        elif self._method == "DELETE":
            if self._task_name != "":
                _, ret = self._delete_task()
            else:
                raise BadRequest("Incorrect Path")  # pragma: no cover
        else:
            raise BadRequest("REST VERB not supported")  # pragma: no cover

        return ret

    def _create_or_alter_task(self) -> Tuple[str, Dict[str, Any]]:
        """CREATE OR ALTER implementation for Task.

        This function collates all SQL statements need to be executed
        to emulate COA and then executes them at once.

        Individual Behavior
        Predecessors:
            Execute a Describe to get the current state of the resource.
            Iterate through the current predecessors and remove any not
            in the provided list.
        Parameters:
            Execute a SHOW command to get the current set of parameters.
            Unset any that are not in the provided list and are not on
            default value already. Set the ones provided to the provided
            values
        When:
            When does not have a default value, so it is unset by setting it
            to 1=1 which is boolean true
        Definition:
            Reset by using a Modify statement. This value is a required value,
            absence of this, throws an error.
        Name:
            can only be set during create. If the entity exists trying to set
            this will throw an error
        Comment / Schedule / Warehouse:
            These are set using the SET statement along with the list of the
            parameters.
        """
        for key in TaskResource.task_prop_list["required"]:
            # Return error if required properties are not present
            if key not in self._prop:
                raise BadRequest(f"Required Property {key} is missing")    # pragma: no cover

        coa_sql = []  # Aggregated SQL to be run at the end
        coa_unset_prop_list = []  # Aggregated  properties to be used with UNSET SQL
        coa_set_prop_list = []  # Aggregated properties to be used with SET SQL

        def _set_predecessors(predecessors: List[str]) -> None:
            remove_all = False
            remove_pred_list = []
            if "predecessors" not in self._prop or not self._prop["predecessors"]:
                remove_all = True

            # Convert the string representation of the list into a list
            if predecessors:
                for pred in predecessors:
                    # assuming naming conventiosn {db}.{schema}.{task_name}
                    task_name_only = parse_table_name(pred)[-1]
                    if remove_all or task_name_only not in (
                        prop_pred.upper() for prop_pred in self._prop["predecessors"]
                    ):
                        remove_pred_list.append(pred)
                if remove_pred_list:
                    remove_pred_sql = f"ALTER TASK {self._task_name} REMOVE AFTER {','.join(remove_pred_list)}"
                    coa_sql.append(remove_pred_sql)
            if not remove_all:
                coa_sql.append(
                    "ALTER TASK {} ADD AFTER {}".format(
                        self._task_name, ",".join(self._prop["predecessors"])
                    )
                )

        def _set_parameters(remote_param_list: Dict[Any, Any]) -> Any:
            unset_all_params = False
            params_to_set = {}
            if "session_parameters" not in self._prop:
                unset_all_params = True
            else:
                params_to_set = self._prop["session_parameters"]

            for param in {*remote_param_list.keys(), *params_to_set}:
                if unset_all_params or (
                    params_to_set and param not in params_to_set
                ):
                    # unset parameters that are not mentioned and are not set to their default values
                    coa_unset_prop_list.append(param)
                elif (
                    params_to_set
                    and param in params_to_set
                    and remote_param_list.get(param) != params_to_set[param]
                ):
                    # set parameters that are mentioned and don't have the expected value
                    coa_set_prop_list.append(f"{param} = {try_single_quote_value(params_to_set[param])}")

        try:
            _, desc_task = self._desc_task()
        except NotFound:
            # Entity does not exist. Fall back to CREATE flow
            # TODO add more specific checks to ensure entity does not exist
            return self._create_task()
        for key in (
            TaskResource.task_prop_list["optional"]
            + TaskResource.task_prop_list["required"]
        ):
            if key == "predecessors":
                _set_predecessors(desc_task["predecessors"])
            elif key == "session_parameters":
                show_params = desc_task["session_parameters"]
                _set_parameters(show_params)
            elif key == "condition":
                if key not in self._prop:
                    # unset when
                    coa_sql.append(f"ALTER TASK {self._task_name} MODIFY WHEN 1=1")
                else:
                    coa_sql.append(
                        f"ALTER TASK {self._task_name} MODIFY WHEN {self._prop[key]}"
                    )
            elif key == "definition":
                if key not in self._prop:
                    raise BadRequest(  # pragma: no cover
                        "Definition needs to be set for an existing resource."
                    )
                elif self._prop[key] != desc_task.get(key):
                    # definition is not already set to the desired value
                    coa_sql.append(
                        f"ALTER TASK {self._task_name} MODIFY AS {self._prop[key]}"
                    )
            elif key == "schedule":
                if key not in self._prop:
                    coa_unset_prop_list.append(key)
                else:
                    if self._prop[key]["schedule_type"] == "MINUTES_TYPE":
                        minutes = self._prop[key]["minutes"]
                        prop_str = f"SCHEDULE = '{minutes} MINUTE'"
                        coa_set_prop_list.append(prop_str)
                    elif len(self._prop[key]) == 3:
                        expr = self._prop[key]["cron_expr"]
                        tz = self._prop[key]["timezone"]
                        prop_str = f"SCHEDULE = 'USING CRON {expr} {tz}'"
                        coa_set_prop_list.append(prop_str)
            elif key == "config":
                if desc_task.get(key) != self._prop.get(key):
                    if not self._prop.get(key):
                        coa_unset_prop_list.append("config")
                    else:
                        prop_str = f"CONFIG = {try_single_quote_value(json.dumps(self._prop.get(key)))}"
                        coa_set_prop_list.append(prop_str)
            elif key in (
                "comment",
                "warehouse",
                "allow_overlapping_execution",
                "error_integration",
                "suspend_task_after_num_failures",
            ):
                if self._prop.get(key) is None:
                    if desc_task.get(key) is not None:
                        coa_unset_prop_list.append(key)
                elif self._prop[key] != desc_task.get(key):
                    value = self._prop[key]
                    if key in (
                        "comment",
                        "error_integration",
                    ):
                        value = try_single_quote_value(value)
                    coa_set_prop_list.append(f"{key.upper()} = {value}")
            elif key == "user_task_managed_initial_warehouse_size" and self._prop.get(
                key
            ) != desc_task.get(key):
                if key in self._prop:
                    raise BadRequest(
                        "Managed Warehouse cannot be changed on an existing resource"
                    )
            elif key == "name" and normalize_name(self._prop[key]) != normalize_name(
                self._task_original_name
            ):
                raise BadRequest(
                    "Rename is not supported with the create_or_update API."
                )

        # Convert all properties to be unset into an UNSET SQL statement
        if coa_unset_prop_list:
            unset_prop_sql = f"ALTER TASK {self._task_name} UNSET "
            unset_prop_sql += ",".join(coa_unset_prop_list)
            coa_sql.append(unset_prop_sql.strip())

        # Convert all properties to be set into a SET SQL statement
        if coa_set_prop_list:
            set_prop_sql = f"ALTER TASK {self._task_name} SET "
            set_prop_sql += ",".join(coa_set_prop_list)
            coa_sql.append(set_prop_sql.strip())
        # All SQL statements to be executed for the PUT REST call should now be in coa_sql.
        # Execute all queries serially
        try:
            for query in coa_sql:
                self.snow_exec.execute(query)
        except RestError as re:
            raise InternalServerError(
                "Could not successfully put the resource on Snowflake. "
                f"{re.msg}",
                re.error_details,
            ) from re

        # We send one success message for the entire batch of SQL statements.
        return str(coa_sql), {"description": "successful"}

    def _show_tasks(self) -> Tuple[str, List[Dict[str, Any]]]:
        sql_str = "SHOW TASKS "
        if "like" in self._query_params:
            sql_str += " LIKE '{}' ".format(self._query_params["like"])

        sql_str += f"in SCHEMA {self._database}.{self._schema} "
        # TODO: This limits the search to a specific schema.
        #  Harsh is going to discuss with REST team how to allow users to search Tasks and other entities without
        #  the scope of database and/or schema.

        if "startsWith" in self._query_params:
            sql_str += "STARTS WITH {} ".format(self._query_params["startsWith"])

        if "rootOnly" in self._query_params and self._query_params["rootOnly"]:
            sql_str += "ROOT ONLY "

        if "showLimit" in self._query_params:
            sql_str += "LIMIT {} ".format(self._query_params["showLimit"])

        tasks = self.snow_exec.execute(sql_str)
        self._validate_task(tasks, deep=False)

        return sql_str, tasks

    def _get_dependents(self) -> Tuple[str, List[Dict[str, Any]]]:
        sql_str = (
            f"SELECT * FROM TABLE(information_schema.task_dependents(task_name=>'{self._task_name}',"
            f"recursive=>true));"
        )
        tasks = self.snow_exec.execute(sql_str)
        new_result = list()
        for task in tasks:
            new_dict: Dict[str, Any] = {}
            new_result.append(new_dict)
            for k, v in task.items():
                new_dict[k.lower()] = v
        new_result = [{k.lower(): v for k, v in task.items()} for task in tasks]
        self._validate_task(new_result, deep=False)
        return sql_str, new_result

    def _get_complete_graphs(self) -> Tuple[str, List[Dict[str, Any]]]:
        error_only = self._query_params.get("errorOnly", False)
        sql_str = f"SELECT * from table(information_schema.complete_task_graphs(error_only=>{error_only}))"
        sql_str = f"{sql_str} where database_name='{self._database}' and schema_name='{self._schema}' and root_task_name='{self._task_original_name}'"

        taskruns = self.snow_exec.execute(sql_str)
        new_result = [
            {k.lower(): v for k, v in taskrun.items()} for taskrun in taskruns
        ]
        for tr in new_result:
            first_error_code = tr.get("first_error_code")
            if first_error_code is not None:
                tr["first_error_code"] = int(first_error_code)
        return sql_str, new_result

    def _get_current_graphs(self) -> Tuple[str, List[Dict[str, Any]]]:
        sql_str = "SELECT * from table(information_schema.current_task_graphs())"

        sql_str = f"{sql_str} where database_name='{self._database}' and schema_name='{self._schema}' and root_task_name='{self._task_original_name}'"

        taskruns = self.snow_exec.execute(sql_str)
        new_result = [
            {k.lower(): v for k, v in taskrun.items()} for taskrun in taskruns
        ]
        return sql_str, new_result

    def _desc_task(self) -> Tuple[str, Dict[str, Any]]:
        sql_str = f"DESC TASK {self._task_name}"
        tasks = self._validate_task(self.snow_exec.execute(sql_str), deep=True)

        return sql_str, tasks[0]

    def _delete_task(self) -> Tuple[str, Dict[str, Any]]:
        sql_str = f"DROP TASK {self._task_name}"
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def _suspend_task(self) -> Tuple[str, Dict[str, Any]]:
        sql_str = f"ALTER TASK {self._task_name} SUSPEND "
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def _execute_task(self) -> Tuple[str, Dict[str, Any]]:
        sql_str = f"EXECUTE TASK {self._task_name}"
        if self._query_params.get("retryLast"):
            sql_str += " RETRY LAST"
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def _resume_task(self) -> Tuple[str, Dict[str, Any]]:
        sql_str = f"ALTER TASK {self._task_name} RESUME "
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def _create_task(self) -> Tuple[str, Dict[str, Any]]:

        sql_str = "CREATE "
        if "createMode" in self._query_params:
            createMode = self._query_params["createMode"]
            # No Op for errorIfExists as that is default behavior
            if createMode == "orReplace":
                sql_str += "OR REPLACE TASK "
            elif createMode == "ifNotExists":
                sql_str += "TASK IF NOT EXISTS "
            elif createMode == "errorIfExists" or not createMode:
                sql_str += "TASK "
            else:
                raise BadRequest(  # pragma: no cover
                    "Unsupported createMode mentioned {}".format(
                        self._query_params["createMode"]
                    )
                )
        else:
            sql_str += "TASK "

        sql_str += "{}.{}.{} ".format(
            self._database, self._schema, normalize_name(self._prop["name"])
        )

        if "warehouse" in self._prop:
            sql_str += "WAREHOUSE={}, ".format(self._prop["warehouse"])
        else:
            if "user_task_managed_initial_warehouse_size" in self._prop:
                sql_str += "USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE= '{}', ".format(
                    self._prop["user_task_managed_initial_warehouse_size"]
                )
        schedule = self._prop.get("schedule", None) if self._prop is not None else None
        if schedule is not None:
            if len(schedule) == 2:
                minutes = schedule["minutes"]
                prop_str = f"SCHEDULE = '{minutes} MINUTE'"
                sql_str += prop_str
            elif len(schedule) == 3:
                expr = schedule["cron_expr"]
                tz = schedule["timezone"]
                prop_str = f"SCHEDULE = 'USING CRON {expr} {tz}'"
                sql_str += prop_str

        overlapping_execution = (
            self._prop.get("allow_overlapping_execution")
            if self._prop is not None
            else None
        )
        if overlapping_execution:
            sql_str += f"ALLOW_OVERLAPPING_EXECUTION = {overlapping_execution}, "

        if "user_task_timeout_ms" in self._prop:
            sql_str += "USER_TASK_TIMEOUT_MS = {}, ".format(
                self._prop["user_task_timeout_ms"]
            )

        if "suspend_task_after_num_failures" in self._prop:
            sql_str += "SUSPEND_TASK_AFTER_NUM_FAILURES = {}, ".format(
                self._prop["suspend_task_after_num_failures"]
            )

        if "error_integration" in self._prop:
            sql_str += "ERROR_INTEGRATION = {}, ".format(
                self._prop["error_integration"]
            )

        if "comment" in self._prop:
            sql_str += " COMMENT = {}, ".format(
                try_single_quote_value(self._prop["comment"])
            )

        if "config" in self._prop:
            sql_str += " CONFIG = {}, ".format(
                try_single_quote_value(json.dumps(self._prop["config"]))
            )

        if "session_parameters" in self._prop:
            # TODO confirm if the params property is received as an array
            for k, v in self._prop["session_parameters"].items():
                if isinstance(v, str) and not is_single_quoted(v):
                    v = f"""'{v.replace("'", "''")}'"""
                sql_str += f"{k} = {try_single_quote_value(v)}, "

        sql_str = sql_str.strip().strip(",")

        if "predecessors" in self._prop and self._prop["predecessors"]:
            predecessors = self._prop["predecessors"]
            new_preds = []
            for pred in predecessors:
                parsed_name = parse_table_name(pred)
                if len(parsed_name) == 1:
                    new_preds.append(
                        f"{self._database}.{self._schema}.{parsed_name[0]}"
                    )
                elif len(parsed_name) == 2:
                    new_preds.append(
                        f"{self._database}.{parsed_name[0]}.{parsed_name[1]}"
                    )
                elif len(parsed_name) == 3:
                    new_preds.append(pred)
                else:
                    raise ValueError(f"Task predecessor {pred} has a wrong format.")

            sql_str += f""" AFTER {','.join(new_preds)} """

        if "condition" in self._prop:
            sql_str += " WHEN {} ".format(self._prop["condition"])

        if "definition" in self._prop:
            sql_str += " AS {} ".format(self._prop["definition"])
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def _validate_task(self, tasks: List[Dict[str, Any]], *, deep: bool = False) -> List[Dict[str, Any]]:
        for t in tasks:
            #  responses are always returned as list of dict
            # sanitize the desc task response
            # convert boolean strings to boolean
            t["name"] = normalize_name(t["name"])
            if (
                "allow_overlapping_execution" in t
                and t["allow_overlapping_execution"] == "null"
            ):
                t["allow_overlapping_execution"] = None
            schedule = t.get("schedule")
            if schedule:
                schedule = schedule.strip()
                if "USING CRON" in schedule.upper():
                    schedule = schedule[len("USING CRON ") :]
                    tz = schedule[schedule.rindex(" ") :].strip()
                    cron_expr = schedule[: schedule.rindex(" ")]
                    t["schedule"] = {"cron_expr": cron_expr, "timezone": tz, "schedule_type": "CRON_TYPE"}
                elif "MINUTE" in schedule.upper():
                    minute_str, _ = schedule.split(" ", 1)
                    try:
                        minute = int(minute_str)
                        t["schedule"] = {"minutes": minute, "schedule_type": "MINUTES_TYPE"}
                    except Exception:
                        raise InternalServerError(
                            f"Invalid Value Generated for Schedule - {schedule}"
                        )
            for prop in t:
                if t[prop] == "false" or t[prop] == "true":
                    t[prop] = False if t[prop] == "false" else True
                elif t[prop] == "[]":
                    t[prop] = []
                elif t[prop] == "null":  # SNOW-1058536: error_integration comes back as 'null'
                    t[prop] = None

            if config := t.get('config'):
                t['config'] = json.loads(config)
            else:
                t['config'] = dict()

            if "predecessors" in t and t["predecessors"]:
                t["predecessors"] = t["predecessors"].strip("][").split(",")
                pred_list = []
                for pred in t["predecessors"]:
                    # Remove trailing new line characters, replace extra quotes, remove trailing spaces
                    task_name = unquote_name(pred.strip("\n").replace('\\"','"').strip())
                    pred_list.append(task_name)
                t["predecessors"] = pred_list
            else:
                t["predecessors"] = []

            if deep:
                params = self.snow_exec.execute(
                    "SHOW PARAMETERS in Task {}.{}.{} ".format(
                        normalize_name(t["database_name"]), normalize_name(t["schema_name"]), normalize_name(t["name"])
                    )
                )

                # Sanitize parameters
                t["session_parameters"] = {}
                for param in params:
                    if param["level"].upper() == "TASK":
                        if (
                            param["key"].lower()
                            == "user_task_managed_initial_warehouse_size"
                        ):
                            t["user_task_managed_initial_warehouse_size"] = param[
                                "value"
                            ].upper()
                            continue
                        elif param["key"].lower() == "user_task_timeout_ms":
                            t["user_task_timeout_ms"] = int(param["value"])
                            continue
                        elif param["key"].lower() == "suspend_task_after_num_failures":
                            t["suspend_task_after_num_failures"] = int(param["value"])
                            continue
                    if param["type"].lower() == "boolean":
                        param["type"] = param["type"].upper()
                        param["value"] = False if param["value"] == "false" else True
                        param["default"] = False if param["default"] == "false" else True
                    elif param["type"].lower() == "number":
                        value = float(param["value"])
                        if value.is_integer():
                            value = int(value)
                        default = float(param["default"])
                        if default.is_integer():
                            default = int(default)
                        param["value"] = value
                        param["default"] = default
                    if param["level"].upper() == "TASK":
                        t["session_parameters"][param["key"]] = param["value"]
                    if param["level"] == "":
                        param["level"] = None
        return tasks
