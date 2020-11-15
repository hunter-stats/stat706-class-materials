import argparse
import pandas
import psycopg2

_conn = None


def get_conn(user: str, host: str, password: str, port: int, dbname: str = None):
    global _conn
    if _conn is not None:
        return _conn
    _conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )
    return _conn


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_host", required=True, type=str)

    parser.add_argument("--db_pass", required=True, type=str)

    parser.add_argument("--db_port", required=False, type=int, default=5432)

    parser.add_argument("--db_name", required=False, type=str, default="movies_data")
