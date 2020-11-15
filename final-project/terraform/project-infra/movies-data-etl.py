import argparse
import pandas
import psycopg2


def get_conn():
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_host", required=True, type=str)

    parser.add_argument("--db_pass", required=True, type=str)

    parser.add_argument("--db_port", required=False, type=int, default=5432)

    parser.add_argument("--db_name", required=False, type=str, default="movies_data")
