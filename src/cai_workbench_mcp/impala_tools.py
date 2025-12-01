## Copyright (c) 2025 Cloudera, Inc. All Rights Reserved.
##
## This file is licensed under the Apache License Version 2.0 (the "License").
## You may not use this file except in compliance with the License.
## You may obtain a copy of the License at http:##www.apache.org/licenses/LICENSE-2.0.
##
## This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
## OF ANY KIND, either express or implied. Refer to the License for the specific
## permissions and limitations governing your use of the file.

import json
import os
from impala.dbapi import connect
import cml.data_v1 as cmldata

CONNECTION_NAME = "Base-Lab - Impala"

# Helper to get Impala connection details from env vars
def get_db_connection():
    return cmldata.get_connection(CONNECTION_NAME)

def execute_query(query: str) -> str:
    conn = None

    # Implement rudimentary SQL injection prevention
    # In this case, we only allow read-only queries
    # This is a very basic check and should be improved for production use
    readonly_prefixes = ["select", "show", "describe", "with"]

    if not query.strip().lower().split()[0] in readonly_prefixes:
        return "Only read-only queries are allowed."

    try:
        conn = get_db_connection()
        cur = conn.get_cursor()
        cur.execute(query)
        if cur.description:
            rows = cur.fetchall()
            result = json.dumps(rows, default=str)
        else:
            conn.commit()
            result = "Query executed successfully."
        cur.close()
        return result
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        if conn:
            conn.close()

def get_schema(database: str) -> str:
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.get_cursor()
        cur.execute("use "+database)
        cur.execute("SHOW TABLES")
        tables = cur.fetchall()
        schema = [table[0] for table in tables]
        return json.dumps(schema)
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        if conn:
            conn.close()
