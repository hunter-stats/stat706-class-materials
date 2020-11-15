import argparse
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
from typing import List, Tuple

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

_conn = None


def get_connection(
    user: str, host: str, password: str, port: int, dbname: str = None
) -> psycopg2.connection:
    global _conn
    if _conn is not None:
        return _conn
    _conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )
    return _conn


def get_csv_length(csvname):
    with open(csvname, "r") as csv:
        length = sum(1 for row in csv)
    return length


def _create_database(dbname: str):
    conn = get_connection()
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    try:
        logging.info(f"creating database {dbname}")
        cursor.execute(f"CREATE DATABASE {dbname}")
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.warn(f"send_to_database: {str(e)}")


def _create_table_sql(tablename: str, schema: List[Tuple]):
    base_str = f"CREATE TABLE IF NOT EXISTS {tablename}(\n"
    for index, column in enumerate(schema):
        (colname, coltype) = column
        base_str += f"{colname} {coltype}"
        if index < len(schema) - 1:
            base_str += ",\n"
    base_str += ");"
    return base_str


def _create_table(dbname: str, tablename: str):
    conn = get_connection(dbname)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    logging.info(f"creating table {tablename}")
    cursor.execute(_create_table_sql(tablename, COLUMN_DEFINITIONS))
    logging.info(f"clearing table {tablename}")
    cursor.execute(f"DELETE FROM {tablename}")
    return


def _initialize_database(dbname: str, tablename: str):
    _create_database(dbname)
    _create_table(dbname, tablename)


def load_csv(
    conn: psycopg2.connection,
    csv_file: str,
    target_table_name: str,
    chunksize: int = 1000,
):
    # create database and table,
    # erasing table's current data (for idempotence)
    _initialize_database(dbname, tablename)

    # TODO: send data to the database
    connection = get_connection(dbname)
    db_cursor = connection.cursor()

    # just for metrics reporting
    rowswritten = 0
    totalrows = get_csv_length(datafilename)

    df_chunked = pd.read_csv(
        datafilename,
        sep="\t",
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
            logging.warn(str(e))
            connection.rollback()
            db_cursor.close()
            connection.close()
            break

        rowswritten += len(df)
        pct_written = 100 * (rowswritten / totalrows)
        logging.info(
            f"send_to_database: {rowswritten} / {totalrows} "
            f"({pct_written:.2f}%) written"
        )
    # TODO: any necessary preparations on the data after loading, and before
    # running the queries to come.

    # it seems some data labeled country = NO
    # has accidentally been labelled 'NONE'
    update_sql = f"UPDATE {tablename} SET country = 'NO' WHERE country IS NULL"
    db_cursor.execute(update_sql)

    # commit the transaction
    # and close
    connection.commit()
    db_cursor.close()
    connection.close()
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_host", required=True, type=str)

    parser.add_argument("--db_pass", required=True, type=str)

    parser.add_argument("--db_port", required=False, type=int, default=5432)

    parser.add_argument("--db_name", required=False, type=str, default="movies_data")
