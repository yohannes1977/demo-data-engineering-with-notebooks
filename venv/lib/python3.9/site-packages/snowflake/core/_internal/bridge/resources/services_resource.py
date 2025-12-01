# Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
import operator

from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import unquote

from snowflake.connector import SnowflakeConnection

from ...utils import normalize_name, try_single_quote_value
from ..rest_errors import BadRequest
from ..snow_request import SnowRequest
from .resource_base import ResourceBase


Results = Union[List[Dict[str, Any]], Dict[str, Any], str, None]


class ServicesResource(ResourceBase):
    actions = [":resume", ":suspend"]
    resource_name = "services"
    service_components = [
        "api",
        "v2",
        "databases",
        "database_name",
        "schemas",
        "schema_name",
        "services",
        "service_name",
        "service_sub_resource",
    ]

    def __init__(self, req: SnowRequest, conn_ob: SnowflakeConnection):
        super().__init__(conn_ob)
        self._action = ""
        self._is_collection = False
        self._service_name = ""
        self._service_full_name = ""
        self._logs = False
        self._status = False
        self._prop = req.body if req.body is not None else dict()
        self._query_params = req.query_params
        self._method = req.method
        self._path_parts = None
        self._database = ""
        self._schema = ""
        self._spec_type = ""

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
        if len(path_parts) < ServicesResource.service_components.index("services") + 1:
            raise BadRequest("Malformed Resource URL")
        elif (
            len(path_parts)
            >= ServicesResource.service_components.index("service_name") + 1
        ):
            self._service_name = normalize_name(unquote(
                path_parts[ServicesResource.service_components.index("service_name")]
            ))
            if (
                len(path_parts)
                > ServicesResource.service_components.index("service_name") + 1
            ):
                service_sub_resource = path_parts[
                    ServicesResource.service_components.index("service_sub_resource")
                ]
                if service_sub_resource == "logs":
                    self._logs = True
                elif service_sub_resource == "status":
                    self._status = True
        elif (
            len(path_parts) == ServicesResource.service_components.index("services") + 1
        ):
            self._is_collection = True
        self._database = normalize_name(unquote(
            path_parts[ServicesResource.service_components.index("database_name")]
        ))
        self._schema = normalize_name(unquote(
            path_parts[ServicesResource.service_components.index("schema_name")]
        ))
        self._service_full_name = (
            f"{self._database}.{self._schema}.{self._service_name}"
        )

    def execute(self) -> Results:
        """
        Routing function

        Using the VERB from the Rest request and the
        metadata collected from the call route to the
        appropriate SQL translation function.
        """
        ret: Results
        if self._method == "PUT":
            _, ret = self.create_or_alter_service()
        elif self._method == "GET":
            if self._is_collection:
                _, ret = self.show_services()
            elif self._logs:
                _, ret = self.show_logs()
            elif self._status:
                _, ret = self.show_status()
            else:
                _, ret = self.desc_service()
        elif self._method == "POST":
            if self._is_collection:
                _, ret = self.create_service()
            elif self._action == "resume":
                _, ret = self.resume_service()
            elif self._action == "suspend":
                _, ret = self.suspend_service()
            else:
                raise BadRequest(" Unsupported Action")
        elif self._method == "DELETE":
            _, ret = self.drop_service()
        else:
            raise BadRequest("Unsupported REST Verb used")
        return ret

    def create_or_alter_service(self) -> Tuple[str, List[Dict[str, Any]]]:
        # TODO SQL NOT FINALIZED
        sql_str = ""
        return sql_str, self.snow_exec.execute(sql_str)

    def show_services(self) -> Tuple[str, List[Dict[str, Any]]]:
        sql_str = "SHOW SERVICES "
        if "like" in self._query_params:
            sql_str = sql_str + " LIKE '{}' ".format(self._query_params["like"])

        if "startsWith" in self._query_params:
            sql_str = sql_str + "STARTS WITH '{}' ".format(
                self._query_params["startsWith"]
            )

        if "showLimit" in self._query_params:
            sql_str = sql_str + "LIMIT {} ".format(self._query_params["showLimit"])
            if "fromName" in self._query_params:
                sql_str = sql_str + "FROM '{}' ".format(self._query_params["fromName"])

        sql_str = sql_str + f"in SCHEMA {self._database}.{self._schema} "
        service_names = map(operator.itemgetter('name'), self.snow_exec.execute(sql_str))
        services = list()
        # Since listing services need to fetch information that's only available
        #  when we DESCRIBE a service we need to describe each of the listed
        #  services
        for sn in service_names:
            self._service_full_name = f"{self._database}.{self._schema}.{sn}"
            _, service_desc = self.desc_service()
            services.append(service_desc)

        return sql_str, services

    def desc_service(self) -> Tuple[str, Dict[str, Any]]:
        sql_str = f"DESC SERVICE {self._service_full_name} "
        response_data = self.snow_exec.execute(sql_str)
        transformed_data = self._transform_services(response_data)
        return sql_str, transformed_data[0] if len(transformed_data) > 0 else dict()

    @staticmethod
    def _transform_services(services: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        new_services = []
        for s in services:
            service = {
                k: v for k, v in s.items()
                if k not in (
                    "spec",
                    "auto_resume",
                )
            }
            result_spec = {
                "spec_type": "from_inline",
                "spec_text": s["spec"],
            }
            service["spec"] = result_spec
            service["auto_resume"] = (s["auto_resume"] == "true")
            if service["comment"] == "":
                service["comment"] = None
            new_services.append(service)
        return new_services

    def show_status(self) -> Tuple[str, Dict[str, Optional[str]]]:
        # TODO Validate that all information is present in the call.
        sql_str = "CALL SYSTEM$GET_SERVICE_STATUS('{}.{}.{}', {})".format(
            self._database,
            self._schema,
            self._service_name,
            self._query_params["timeout"],
        )
        response_data: List[Dict[str, Optional[str]]] = self.snow_exec.execute(sql_str)
        # SYSTEM$GET_SERVICE_STATUS column needs to lower case'd
        return sql_str, {k.lower(): v for k,v in response_data[0].items()}

    def show_logs(self) -> Tuple[str, Dict[str, Optional[str]]]:
        # TODO Validate that all information is present in the call.
        sql_str = "CALL SYSTEM$GET_SERVICE_LOGS('{}.{}.{}','{}', '{}')".format(
            self._database,
            self._schema,
            self._service_name,
            self._query_params["instanceId"],
            self._query_params["containerName"],
        )
        response_data = self.snow_exec.execute(sql_str)
        # SYSTEM$GET_SERVICE_LOGS column needs to lower case'd
        return sql_str, {k.lower(): v for k,v in response_data[0].items()}

    def create_service(self) -> Tuple[str, Dict[str, Any]]:
        sql_str = "CREATE "
        create_mode = self._query_params.get("createMode")
        if create_mode == "orReplace":
            sql_str = sql_str + "OR REPLACE "
        sql_str = sql_str + "SERVICE "
        if create_mode == "ifNotExists":
            sql_str = sql_str + "IF NOT EXISTS "

        sql_str = (
            sql_str + " " + f"{self._database}.{self._schema}.{self._prop['name']}"
        )
        if "compute_pool" in self._prop:
            sql_str = sql_str + " IN COMPUTE POOL {}".format(self._prop["compute_pool"])
        if "spec" in self._prop:
            if self._prop["spec"]["spec_type"] == "from_file":
                sql_str = sql_str + " FROM @{stage} SPECIFICATION_FILE = '{file}'".format(stage=self._prop["spec"]["stage"], file=self._prop["spec"]["spec_file"])
            elif self._prop["spec"]["spec_type"] == "from_inline":
                sql_str = sql_str + " FROM SPECIFICATION '{}'".format(self._prop["spec"]["spec_text"])
        if "min_instances" in self._prop:
            sql_str = sql_str + " MIN_INSTANCES = {}".format(
                self._prop["min_instances"]
            )
        if "max_instances" in self._prop:
            sql_str = sql_str + " MAX_INSTANCES = {}".format(
                self._prop["max_instances"]
            )
        if "comment" in self._prop:
            sql_str += f" COMMENT = {try_single_quote_value(self._prop['comment'])}"

        return sql_str, self.snow_exec.execute(sql_str)[0]

    def suspend_service(self) -> Tuple[str, Dict[str, Any]]:
        if "ifExists" in self._query_params and self._query_params["ifExists"]:
            sql_str = f"ALTER SERVICE IF EXISTS {self._service_full_name} SUSPEND"
        else:
            sql_str = f"ALTER SERVICE {self._service_full_name} SUSPEND"

        return sql_str, self.snow_exec.execute(sql_str)[0]

    def resume_service(self) -> Tuple[str, Dict[str, Any]]:
        if "ifExists" in self._query_params and self._query_params["ifExists"]:
            sql_str = f"ALTER SERVICE IF EXISTS {self._service_full_name} RESUME"
        else:
            sql_str = f"ALTER SERVICE {self._service_full_name} RESUME"

        return sql_str, self.snow_exec.execute(sql_str)[0]

    def drop_service(self) -> Tuple[str, Dict[str, Any]]:
        if "ifExists" in self._query_params and self._query_params["ifExists"]:
            sql_str = f"DROP SERVICE IF EXISTS {self._service_full_name}"
        else:
            sql_str = f"DROP SERVICE {self._service_full_name}"

        return sql_str, self.snow_exec.execute(sql_str)[0]
