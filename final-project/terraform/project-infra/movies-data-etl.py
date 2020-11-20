import argparse
from collections import OrderedDict
from contextlib import contextmanager
import io
import json
import logging
import pandas as pd
import psycopg2
from psycopg2 import pool
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
from typing import List, Tuple

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def get_pg_type(pandas_dtype: str) -> str:
    if pandas_dtype in ["object"]:
        return "JSONB"
    elif pandas_dtype in ["float64"]:
        return "DECIMAL(10, 2)"
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
    for colname, coltype in schema.items():
        pg_type = get_pg_type(str(coltype))
        base_str += f"{colname} {pg_type}"
        if index < len(schema) - 1:
            base_str += ",\n"
        index += 1
    base_str += ");"
    return base_str


def create_table(conn: "psycopg2.connection", csv_file: str, tablename: str):
    schema = OrderedDict(pd.read_csv(csv_file, nrows=20).dtypes)
    create_sql = create_table_sql(tablename, schema)
    conn.cursor().execute(create_sql)


def load_csv(csv_file: str):
    tablename = csv_file.replace(".csv", "")

    logging.info(f"Creating table {tablename}")
    with get_connection() as conn:
        create_table(conn, csv_file, tablename)

    logging.info(f"Loading CSV {csv_file} into {tablename}")
    with get_connection() as conn:
        stream_csv_to_table(conn, csv_file, tablename)


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

    df_chunked = pd.read_csv(
        csv_file,
        chunksize=chunksize,
        keep_default_na=False,
        na_values=["NONE", "NULL"],
    )

    for df in df_chunked:
        # json stringify columns that were parser
        for colname, coltype in df.dtypes.iteritems():
            if get_pg_type(coltype) != 'JSONB':
                continue
                
            try:
                df[colname].transform(json.loads)
            except Exception:
                pass

        buffer = io.StringIO()
        df.to_csv(buffer, header=False, index=False, sep="\t", na_rep="NULL")
        buffer.seek(0)

        try:
            db_cursor.copy_from(
                buffer, tablename, columns=list(df.columns), sep="\t", null="NULL"
            )
        except Exception as e:
            logging.error(str(e))
            connection.rollback()
            raise(e)

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

    # with get_connection(ISOLATION_LEVEL_AUTOCOMMIT) as con:
    #     try:
    #         create_database(con, args.dbname)
    #     except Exception as e:
    #         logging.warning(f"{e}")

    CSV_FILES = ["movies_metadata.csv", "ratings.csv", "links.csv"]

    for csv_file in CSV_FILES:
        load_csv(csv_file)
