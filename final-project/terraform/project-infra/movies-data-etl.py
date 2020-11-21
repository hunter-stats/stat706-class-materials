import argparse
from collections import OrderedDict
from contextlib import contextmanager
from enum import Enum
import io
import json
import logging
import pandas as pd
import psycopg2
from psycopg2 import pool
from psycopg2.extras import register_json, Json
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, register_adapter
import sys
from typing import List, Tuple

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

register_adapter(dict, Json)

class PostgresType(Enum):
    TEXT = "TEXT"
    JSONB = "JSONB"
    INT = "INTEGER"
    DEC = "DECIMAL(15, 2)"
    BIGINT = "BIGINT"


def to_float(x):
    try:
        return float(x)
    except Exception as e:
        logging.warning(str(e))
        return None

def correct_json(bad_json: str):
    try:
        good_json = bad_json.replace("\'", '\"')
        obj = json.loads(good_json)
        return obj
    except Exception as e:
        logging.warning(str(e))
        return None

SCHEMAS = {
    "movies_metadata": {
        # TODO(nickhil): could just make this text
        # this is giving me trouble
        "genres": (PostgresType.TEXT, correct_json),
        "imdb_id": (PostgresType.TEXT, None),
        "revenue": (PostgresType.DEC, to_float),
        "budget": (PostgresType.DEC, to_float),
        "original_title": (PostgresType.TEXT, None)
        # TODO(nickhil): this column is causing problems
        # "overview": PostgresType.TEXT,
    }
}


def get_pg_type(pandas_dtype: str) -> str:
    if pandas_dtype in ["object"]:
        return "TEXT"
    elif pandas_dtype in ["float64"]:
        return "DECIMAL(15, 6)"
    elif pandas_dtype in ["int64"]:
        return "INTEGER"
    elif pandas_dtype in ["bool"]:
        return "BOOLEAN"
    return "TEXT"


@contextmanager
def get_connection(isolation_level: str = None):
    global CONNECTION_POOL
    con = CONNECTION_POOL.getconn()
    if isolation_level:
        con.set_isolation_level(isolation_level)
    try:
        yield con
    finally:
        con.reset()
        CONNECTION_POOL.putconn(con)


def get_csv_length(csvname):
    with open(csvname, "r") as csv:
        length = sum(1 for row in csv)
    return length


def create_database(conn: "psycopg2.connection", dbname: str):
    conn.cursor().execute(f"CREATE DATABASE {dbname}")


def create_table_sql(tablename: str, schema: OrderedDict):
    base_str = f"DROP TABLE IF EXISTS {tablename};\n"
    base_str += f"CREATE TABLE IF NOT EXISTS {tablename}(\n"
    index = 0
    for colname, (coltype, _) in schema.items():
        base_str += f"{colname} {coltype.value}"
        if index < len(schema) - 1:
            base_str += ",\n"
        index += 1
    base_str += ");"
    return base_str


def create_table(conn: "psycopg2.connection", csv_file: str, tablename: str):
    schema = SCHEMAS[tablename]
    create_sql = create_table_sql(tablename, schema)
    conn.cursor().execute(create_sql)


def load_csv(csv_file: str):
    tablename = csv_file.replace(".csv", "")

    logging.info(f"Creating table {tablename}")
    with get_connection() as conn:
        create_table(conn, csv_file, tablename)
        conn.commit()

    logging.info(f"Loading CSV {csv_file} into {tablename}")
    with get_connection() as conn:
        stream_csv_to_table(conn, csv_file, tablename)
        conn.commit()


def stream_csv_to_table(
    connection: "psycopg2.connection",
    csv_file: str,
    tablename: str,
    chunksize: int = 1000,
):
    db_cursor = connection.cursor()

    # just for metrics reporting
    rows_written = 0
    total_rows = get_csv_length(csv_file)
    columns = SCHEMAS[tablename].keys()
    schema = SCHEMAS[tablename]
    df_chunked = pd.read_csv(csv_file, chunksize=chunksize, usecols=columns)

    for df in df_chunked:
        for col in columns:
            if schema[col][1] is None:
                continue
            print(df[col])
            df[col] = df[col].apply(schema[col][1])
            print(df[col])
        buffer = io.StringIO()
        df.to_csv(buffer, header=False, index=False, sep="|", na_rep="NULL")
        buffer.seek(0)
        try:
            db_cursor.copy_from(
                buffer, tablename, columns=list(df.columns), sep="|", null="NULL"
            )
        except Exception as e:
            logging.error(str(e))
            connection.rollback()
            raise e

        rows_written += len(df)
        pct_written = 100 * (rows_written / total_rows)

        logging.info(
            f"{tablename} {rows_written} / {total_rows} ({pct_written:.2f}%) written"
        )

    # commit the transaction
    # and close
    connection.commit()
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--db_user", required=True, type=str)
    parser.add_argument("--db_host", required=True, type=str)
    parser.add_argument("--db_pass", required=True, type=str)
    parser.add_argument("--db_port", required=False, type=int, default=5432)
    parser.add_argument("--db_name", required=False, type=str, default="movies_data")

    args = parser.parse_args()

    MIN_CONNECTIONS = 1
    MAX_CONNECTIONS = 3

    CONNECTION_POOL = pool.SimpleConnectionPool(
        MIN_CONNECTIONS,
        MAX_CONNECTIONS,
        host=args.db_host,
        # database=args.db_name,
        user=args.db_user,
        password=args.db_pass,
        port=args.db_port,
    )

    CSV_FILES = [
        # genres, budget, revenue, imdbid
        "movies_metadata.csv",
        # userid, movieid, ratings
        "ratings.csv",
        # *
        "links.csv",
    ]

    for csv_file in CSV_FILES:
        load_csv(csv_file)
