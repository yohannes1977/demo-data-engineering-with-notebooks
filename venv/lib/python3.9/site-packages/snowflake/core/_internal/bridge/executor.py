import typing

from typing import Any, Dict, List, Optional

from snowflake.connector import DictCursor, SnowflakeConnection, errorcode, errors

from .rest_errors import (
    BadGateway,
    BadRequest,
    Conflict,
    Forbidden,
    GatewayTimeout,
    InternalServerError,
    NotFound,
    ServiceUnavailable,
    UnauthorizedRequest,
)


class SnowExecute:
    def __init__(self, conn: SnowflakeConnection):
        self.conn = conn

    def execute(
        self,
        sql: str,
        desired_properties: Optional[List[str]] = None,
        num_statements: int = 1,
    ) -> List[Dict[str, Any]]:
        """
        Method to execute the SQL using snowflake python connector.

        * Returns List[Dict[str, Any]] for sql which returns one or more records.

        * Returns [{"description": "successful"}] for operations with no data such as create or alter.

        * Return empty list if the output is empty. example: show tasks like '<invalid pattern>'

        If desired_properties is passed, it will construct the output dictionary using the desired properties
           and ignores other fields from the cursor's resultset.

        Raises Exception if the requests are not successful.

        Usage:
            executor.execute("show tasks",
                desired_properties=["name", "warehouse", "schedule", "comment", "params", "definition", "predecessors", "copyGrants", "when", "created_on", "id", "owner",
                "state", "condition", "allow_overlapping_execution", "error_integration", "last_committed_on", "last_suspended_on"])

            returns [{"name": "", "warehouse": "", "schedule": ""}, {..}]

        """
        # TODO: connector has a bug in this, mypy should find that DictCursor gets returned here
        cur = self.conn.cursor(DictCursor)
        try:
            self._execute_internal(cur, sql, num_statements=num_statements)  # type: ignore
            rows = cur.fetchall()
            output = self._construct_response(rows, desired_properties)  # type: ignore[arg-type]
            return output
        finally:
            cur.close()

    def execute_many(
            self,
            sql: str,
            num_statements: int = 1,
    ) -> List[List[Dict[str, Any]]]:
        """
        Method for executing multiple SQL statements separated by a semi-colon (;) using the Snowflake Python connector.
        example sql: "show tasks; select 1"
        output: List[List[Dict[str, Any]]]
        For each sql:
            * Adds List[Dict[str, Any]] dictionary to the output if sql returns 1 or more rows.

            * Adds [{"description": "successful"}] for sql operations with no data such as create or alter.

            * Adds empty list if the output is empty. example: show tasks like '<invalid pattern>'

        Raises Exception if the requests are not successful.

        Usage:
            executor.execute_many("show tasks; select 1")
            returns [[{"name": "", "warehouse": "", "schedule": ""}, {..}], [{"1": 1}]
        """
        with self.conn.cursor(DictCursor) as cur:
            self._execute_internal(
                cur,  # type: ignore
                sql,
                statement_parms={"MULTI_STATEMENT_COUNT": "0"},
                num_statements=num_statements
            )
            output = []
            while True:
                rows = cur.fetchall()
                # TODO: connector's type-hints could be better here
                updated_rows = self._construct_response(rows, None)  # type: ignore[arg-type]
                output.append(updated_rows)
                if cur.nextset() is None:
                    break
            return output

    def _execute_internal(
        self,
        cursor: DictCursor,
        sql: str,
        statement_parms: Optional[Dict[str, str]] = None,
        num_statements: int = 1,
    ) -> None:
        """
        Method for handling SQL execution. This method also handles any errors returned by the Python connector and translates them into RestErrors.
        - cursor: The cursor to be used for executing sql
        - statement_parms: optional parameters to be passed to snowflake backend.
        """
        try:
            cursor.execute(sql, _statement_params=statement_parms, num_statements=num_statements)
        except errors.InterfaceError as e:
            error_details = {
                "errno": e.errno,
                "query": sql,
                "sqlstate": e.sqlstate,
                "sfqid": e.sfqid,
            }
            raise BadRequest(e.msg, error_details)


        except errors.RevocationCheckError as e:
            error_details = {
                "errno": e.errno,
                "query": sql,
                "sqlstate": e.sqlstate,
                "sfqid": e.sfqid,
            }

            if (
                errorcode.ER_OCSP_URL_INFO_MISSING
                <= e.errno
                <= errorcode.ER_INVALID_SSD
            ):
                # OCSP related issues.
                raise UnauthorizedRequest(e.msg, error_details)

            raise InternalServerError(e.msg, error_details)

        except errors.OperationalError as e:
            error_details = {
                "errno": e.errno,
                "query": sql,
                "sqlstate": e.sqlstate,
                "sfqid": e.sfqid,
            }
            if e.errno in [errorcode.ER_INVALID_STAGE_FS, errorcode.ER_FILE_NOT_EXISTS]:
                # file transfer requests, if files are not found.
                raise BadRequest(e.msg, error_details)
            if e.errno in [
                errorcode.ER_FAILED_TO_REQUEST,
                errorcode.ER_FAILED_TO_UPLOAD_TO_STAGE,
                errorcode.ER_FAILED_TO_CONNECT_TO_DB,
            ]:
                # s3 auth issues.
                raise BadGateway(e.msg, error_details)

        except (
            errors.IntegrityError,
            errors.InternalServerError,
            errors.MissingDependencyError,
            errors.RequestExceedMaxRetryError,
            errors.InternalError,
        ) as e:
            error_details = {
                "errno": e.errno,
                "query": sql,
                "sqlstate": e.sqlstate,
                "sfqid": e.sfqid,
            }
            raise InternalServerError(e.msg, error_details)

        except (errors.NotSupportedError, errors.ProgrammingError) as e:
            error_details = {
                "errno": e.errno,
                "query": sql,
                "sqlstate": e.sqlstate,
                "sfqid": e.sfqid,
            }

            if e.errno == 2002:
                # error code for resource already exists.
                raise Conflict(e.msg, error_details)
            elif e.errno == 2003:
                # error code for resource doesn't exists.
                raise NotFound(e.msg, error_details)
            raise BadRequest(e.msg, error_details)

        except errors.DatabaseError as e:
            # Occurs during authentication errors
            error_details = {
                "errno": e.errno,
                "query": sql,
                "sqlstate": e.sqlstate,
                "sfqid": e.sfqid,
            }
            raise UnauthorizedRequest(e.msg, error_details)

        except errors.GatewayTimeoutError as e:
            error_details = {
                "errno": e.errno,
                "query": sql,
                "sqlstate": e.sqlstate,
                "sfqid": e.sfqid,
            }
            raise GatewayTimeout(e.msg, error_details)

        except errors.ServiceUnavailableError as e:
            error_details = {
                "errno": e.errno,
                "query": sql,
                "sqlstate": e.sqlstate,
                "sfqid": e.sfqid,
            }
            raise ServiceUnavailable(e.msg, error_details)

        except errors.ForbiddenError as e:
            error_details = {
                "errno": e.errno,
                "query": sql,
                "sqlstate": e.sqlstate,
                "sfqid": e.sfqid,
            }
            raise Forbidden(e.msg, error_details)

        except Exception as e:
            raise InternalServerError(str(e))

    def _construct_response(
        self,
        rows: typing.List[typing.Dict[Any, Any]],
        desired_properties: typing.Optional[typing.List[str]],
    ) -> List[Dict[str, Any]]:
        if len(rows) == 1 and len(rows[0]) == 1 and "status" in rows[0]:
            # In case of success response only return description field.
            return [{"description": "successful"}]
        elif len(rows) >= 1:
            # handle multiple rows
            if desired_properties:
                rows = [
                    {key: row.get(key, None) for key in desired_properties}
                    for row in rows
                ]
            return rows
        else:
            # if the result is empty for commands such as show tasks like '<invalid pattern>'
            return []  # empty list.
