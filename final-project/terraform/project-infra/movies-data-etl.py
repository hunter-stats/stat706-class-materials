import argparse
from contextlib import contextmanager
import logging
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
from typing import List, Tuple

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

CONNECTION_POOL = None


@contextmanager
def get_connection(isolation_level: str = None):
    con = CONNECTION_POOL.getconn()
    if isolation_level:
        con.set_isolation_level(isolation_level)
    try:
        yield con
    finally:
        conn.reset()
        CONNECTION_POOL.putconn(con)


def get_csv_length(csvname):
    with open(csvname, "r") as csv:
        length = sum(1 for row in csv)
    return length


def create_database(conn: psycopg2.connection, dbname: str):

    logging.info(f"creating database {dbname}")
    cursor.execute(f"CREATE DATABASE {dbname}")
    conn.commit()


def _create_table_sql(tablename: str, schema: List[Tuple]):
    base_str = f"DROP TABLE IF EXISTS {tablename}\n"
    base_str += f"CREATE TABLE IF NOT EXISTS {tablename}(\n"
    for index, column in enumerate(schema):
        (colname, coltype) = column
        base_str += f"{colname} {coltype}"
        if index < len(schema) - 1:
            base_str += ",\n"
    base_str += ");"
    return base_str


def create_table(dbname: str, tablename: str):
    logging.info(f"creating table {tablename}")
    cursor.execute(_create_table_sql(tablename, COLUMN_DEFINITIONS))
    return


def load_csv(csv_file: str, tablename: str):
    with get_connection(ISOLATION_LEVEL_AUTOCOMMIT) as conn:
        cursor = conn.cursor()
        create_table(conn, tablename)

    with get_connection() as conn:
        stream_csv_to_table(conn, csv_file, tablename)


def stream_csv_to_table(
    conn: psycopg2.connection,
    csv_file: str,
    tablename: str,
    chunksize: int = 1000,
):
    db_cursor = connection.cursor()

    # just for metrics reporting
    rowswritten = 0
    totalrows = get_csv_length(datafilename)

    df_chunked = pd.read_csv(
        datafilename,
        chunksize=chunksize,
        keep_default_na=False,
        na_values=["NONE", "NULL"],
    )

    logging.info(f"writing data from {datafilename} " f"to postgres table {tablename}")

    int_cols = [
        col for col, coltype in COLUMN_PYDATA_DEFINITIONS.items() if coltype is int
    ]
    for df in df_chunked:

        df[int_cols] = df[int_cols].fillna(-1)
        df[int_cols] = df[int_cols].apply(pd.to_numeric, downcast="integer")

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
            db_cursor.close()
            connection.close()
            break

        rowswritten += len(df)
        pct_written = 100 * (rowswritten / totalrows)
        logging.info(
            f"{tablename} {rowswritten} / {totalrows} " f"({pct_written:.2f}%) written"
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

    CONNECTION_POOL = psycopg2.SimpleConnectionPool(
        MIN_CONNECTIONS,
        MAX_CONNECTIONS,
        host=args.db_host,
        database=args.db_name,
        user=args.db_user,
        password=args.db_pass,
        port=args.db_port,
    )
