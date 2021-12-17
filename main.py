import sys
import psycopg2
from step1 import step1_db
from step1 import step1_csv


def main(flag):
    if flag == 1:
        try:
            conn = psycopg2.connect(dbname="northwind", user="northwind_user", password="thewindisblowing")
        except psycopg2.Error as err:
            print(f"=> Error trying to connect to postgres database: {err}")
            sys.exit()

        step1_db.process_postgress_db(conn, "2021-11-04")

    elif flag == 2:
        step1_csv.process_csv_data()


if __name__ == "__main__":
    main(2)
