# Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
from __future__ import annotations

from typing import Any
from urllib.parse import unquote

from snowflake.connector import SnowflakeConnection

from ...utils import normalize_name
from ..rest_errors import BadRequest, NotFound
from ..snow_request import SnowRequest
from .resource_base import ResourceBase


class ImageRepositoriesResource(ResourceBase):

    resource_name = "imagerepositories"
    image_repo_prop_list = {"required": ["name"], "optional": []}
    image_repo_components = [
        "api",
        "v2",
        "databases",
        "database_name",
        "schemas",
        "schema_name",
        "image_repositories",
        "image_repo_name",
    ]

    def __init__(self, req: SnowRequest, conn_ob: SnowflakeConnection):
        super().__init__(conn_ob)
        self._database = ""
        self._schema = ""
        self._is_collection = False
        self._prop = req.body if req.body is not None else dict()
        self._query_params = req.query_params
        self._method = req.method
        self._repo_name = ""

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
        self.is_collection = False
        self._database = normalize_name(unquote(
            path_parts[
                ImageRepositoriesResource.image_repo_components.index("database_name")
            ]
        ))
        self._schema = normalize_name(unquote(
            path_parts[
                ImageRepositoriesResource.image_repo_components.index("schema_name")
            ]
        ))
        if (
            len(path_parts)
            < ImageRepositoriesResource.image_repo_components.index(
                "image_repositories"
            )
            + 1
        ):
            raise BadRequest("Malformed Resource URL")
        elif (
            len(path_parts)
            >= ImageRepositoriesResource.image_repo_components.index("image_repo_name")
            + 1
        ):
            self._repo_name = normalize_name(unquote(
                path_parts[
                    ImageRepositoriesResource.image_repo_components.index(
                        "image_repo_name"
                    )
                ]
            ))
        elif (
            len(path_parts)
            == ImageRepositoriesResource.image_repo_components.index(
                "image_repositories"
            )
            + 1
        ):
            self._is_collection = True

    def execute(self) -> list[dict[str, Any]] | dict[str, Any]:
        """
        Routing function

        Using the VERB from the Rest request and the
        metadata collected from the call route to the
        appropriate SQL translation function.
        """
        ret: list[dict[str, Any]] | dict[str, Any] = dict()
        if self._method == "PUT":
            raise NotFound("PUT on image repository is not supported")
        elif self._method == "GET":
            if self._is_collection:
                _, ret = self._show_image_repo()
            elif self._repo_name != "":
                _, ret = self._desc_image_repo()
        elif self._method == "POST":
            _, ret = self._create_image_repo()
        elif self._method == "DELETE":
            _, ret = self._drop_image_repo()
        else:
            raise BadRequest("Unsupported REST Verb used")

        return ret

    def _desc_image_repo(self) -> tuple[str, dict[str, Any]]:
        self._query_params["like"] = self._repo_name
        sql_str, resp = self._show_image_repo()
        if not resp:
            raise NotFound(f"Image repository {self._repo_name} does not exist or not authorized.")
        return sql_str, resp[0]

    def _show_image_repo(self) -> tuple[str, list[dict[str, Any]]]:
        sql_str = "SHOW IMAGE REPOSITORIES "
        if "like" in self._query_params.keys():
            sql_str = sql_str + " LIKE '%{}%' ".format(self._query_params["like"])
        sql_str = sql_str + f"in SCHEMA {self._database}.{self._schema} "
        return sql_str, self.snow_exec.execute(sql_str)

    def _create_image_repo(self) -> tuple[str, dict[str, Any]]:
        # TODO - WITH clause and TAGS and COMMENTS are not supported
        # since the spec doesn't support it yet.
        sql_str = "CREATE "
        create_mode = self._query_params.get("createMode")
        if create_mode == "orReplace":
            sql_str = sql_str + "OR REPLACE "
        sql_str = sql_str + "IMAGE REPOSITORY "
        if create_mode == "ifNotExists":
            sql_str = sql_str + "IF NOT EXISTS "
        sql_str = sql_str + "{}.{}.{}".format(
            self._database, self._schema, self._prop["name"]
        )
        return sql_str, self.snow_exec.execute(sql_str)[0]

    def _drop_image_repo(self) -> tuple[str, dict[str, Any]]:
        sql_str = "DROP IMAGE REPOSITORY "
        if "ifExists" in self._query_params.keys() and self._query_params["ifExists"]:
            sql_str = sql_str + "IF EXISTS "
        sql_str = sql_str + f"{self._database}.{self._schema}.{self._repo_name}"
        return sql_str, self.snow_exec.execute(sql_str)[0]
