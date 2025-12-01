# Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

from snowflake.connector import SnowflakeConnection


class SnowExecute:
    def __init__(self, conn: SnowflakeConnection):
        self.conn = conn

    def execute(self, sql: str) -> str:
        return "Success"
