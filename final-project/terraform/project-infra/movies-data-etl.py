import argparse
from collections import OrderedDict
from contextlib import contextmanager
import datetime as dt
from dateutil.parser import parse
from enum import Enum
import io
import json
import logging
import os
import pandas as pd
import psycopg2
from psycopg2 import pool
from psycopg2.extras import register_json, Json
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, register_adapter
import sys
from typing import List, Tuple, Dict

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

register_adapter(dict, Json)


class PostgresType(Enum):
    TEXT = "TEXT"
    JSONB = "JSONB"
    INT = "INTEGER"
    DEC = "DECIMAL(15, 2)"
    SMALL_DEC = "DECIMAL(2,1)"
    BIGINT = "BIGINT"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"


def to_date(x):
    try:
        parsed_date = str(parse(x).date())
        return parsed_date
    except Exception as e:
        logging.warning(f"to_date {e}")
        return None


def strip_prefix(x: str):
    try:
        return x.replace("tt0", "")
    except Exception as e:
        logging.warning(str(e))
        return None


def to_float(x):
    try:
        return float(x)
    except Exception as e:
        logging.warning(f"to_float: {e}")
        return None


def safe_int(x):
    try:
        return int(x)
    except Exception as e:
        logging.warning(f"to_int: {e}")
        return None


def correct_json(bad_json: str):
    try:
        good_json = bad_json.replace("'", '"')
        obj = json.loads(good_json)
        return obj
    except Exception as e:
        logging.warning(f"correct_json {e}")
        return None


SCHEMAS: Dict[str, Dict[str, Tuple]] = {
    "movies_metadata": {
        # TODO(nickhil): could just make this text
        # this is giving me trouble
        "genres": (PostgresType.TEXT, correct_json),
        # TODO(nickhil): why does strip prefix throw an
        # error here
        "imdb_id": (PostgresType.TEXT, None),
        # TODO(nickhil): these aren't showing up
        "revenue": (PostgresType.TEXT, None),
        "budget": (PostgresType.TEXT, None),
        "original_title": (PostgresType.TEXT, None),
        # TODO(nickhil): this column is causing problems
        # "overview": PostgresType.TEXT,
        # TODO(nickhil): figure out why this was fucking up
        # when I type converted it, even back to a string
        "release_date": (PostgresType.TEXT, None),
    },
    "ratings": {
        "rating": (PostgresType.SMALL_DEC, None),
        "userId": (PostgresType.BIGINT, None),
        "movieId": (PostgresType.BIGINT, None),
        "timestamp": (PostgresType.TIMESTAMP, dt.datetime.utcfromtimestamp),
    },
    "links": {
        "movieId": (PostgresType.TEXT, None),
        "imdbId": (PostgresType.TEXT, None),
        "tmdbId": (PostgresType.TEXT, None),
    },
}


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


def create_table_sql(tablename: str, schema: Dict[str, Tuple]):
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
    chunksize: int = 5000,
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
            transformation = schema[col][1]
            if transformation is None:
                continue
            df[col] = df[col].apply(schema[col][1])

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


def create_data_table():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM movie_genres;")
        genres = cursor.fetchall()
        logging.info(f"Found genres {genres}")

    genre_names = [g[1] for g in genres]
    genre_cols = (",\n").join(
        [
            f"\tgenre_{name.replace(' ', '_').lower()} BOOLEAN DEFAULT FALSE"
            for name in genre_names
        ]
    )
    create_sql = f"""
    DROP TABLE project_data; 
    CREATE TABLE project_data (
        imdb_id INTEGER,
        movie_id INTEGER,
        average_rating DEC(2,1),
        revenue DEC(12, 1),
        budget DEC(32, 1),
        original_title TEXT,
        release_date TEXT,
        {genre_cols}
    );"""

    with get_connection() as conn:
        cursor = conn.cursor()
        logging.info(f"Running: {create_sql}")
        cursor.execute(create_sql)
        conn.commit()
    return


def update_movie_genre(conn: "psycopg2.connection", movie: Tuple):
    cursor = conn.cursor()
    genres_list = json.loads(movie[1].replace("'", '"'))
    movie_id = movie[0]
    movie_name = movie[2]

    if movie_id is None:
        logging.error(f"Could not find id for movie {movie_name}, returning")
        return
    logging.info(f"Found genres {genres_list} for movie {movie_id} {movie_name}")
    if not genres_list:
        return
    genres = [genre_obj["name"].replace(" ", "_").lower() for genre_obj in genres_list]
    genre_updates = ("\n, ").join([f"genre_{g} = TRUE\n" for g in genres])
    update_sql = f"""
        UPDATE project_data
        SET {genre_updates}
        WHERE imdb_id = {movie_id}
    """
    logging.info(f"Running: {update_sql}")
    cursor.execute(update_sql)


def update_movie_genres():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""SELECT cleaned_imdb_id, genres, original_title
                FROM movies_metadata;"""
        )
        movie_genres = cursor.fetchall()

    with get_connection() as conn:
        for movie in movie_genres:
            update_movie_genre(conn, movie)
        conn.commit()


def download_project_data():
    with get_connection() as conn:
        df = pd.read_sql("SELECT * FROM project_data;", conn)
        df.to_csv("project_data.csv", index=False)


def clean_dates(project_data_file: str):
    project_data = pd.read_csv(project_data_file)
    project_data["release_date"] = project_data["release_date"].apply(to_date)
    project_data.to_csv("cleaned_project_data.csv", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--db_user", required=False, default=os.environ.get("DB_USER"), type=str
    )
    parser.add_argument(
        "--db_host", required=False, default=os.environ.get("DB_HOST"), type=str
    )
    parser.add_argument(
        "--db_pass", required=False, default=os.environ.get("DB_PASS"), type=str
    )
    parser.add_argument(
        "--db_port", required=False, default=os.environ.get("DB_PORT"), type=int
    )

    parser.add_argument(
        "--command",
        choices=[
            "load-data",
            "create-project-table",
            "update-genres",
            "download-data",
            "clean-dates",
        ],
        required=True,
        type=str,
    )

    args = parser.parse_args()

    MIN_CONNECTIONS = 1
    MAX_CONNECTIONS = 3

    CONNECTION_POOL = pool.SimpleConnectionPool(
        MIN_CONNECTIONS,
        MAX_CONNECTIONS,
        host=args.db_host,
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
    if args.command == "load-data":
        for csv_file in CSV_FILES:
            load_csv(csv_file)
    elif args.command == "create-project-table":
        create_data_table()
    elif args.command == "update-genres":
        update_movie_genres()
    elif args.command == "download-data":
        download_project_data()
    elif args.command == "clean-dates":
        clean_dates("./project_data.csv")
