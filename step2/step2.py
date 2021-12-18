import json
import psycopg2
from psycopg2 import sql
from pathlib import Path

DELETE_TABLE_CONTENT = """
    DELETE FROM {table};
    """

INSERT_INTO_TABLE = """
    INSERT INTO {table}
    VALUES ({values});
    """

SELECT_ALL_JOIN_ORDERS_ORDER_DETAILS = """
    SELECT (orders).*, (order_details).* 
    FROM orders 
    INNER JOIN order_details 
    ON orders.order_id = order_details.order_id;
    """

conn = psycopg2.connect(dbname="solution", user="solution_user", password="hereitis", port=5438)


def process_data_to_db(user_date: str):
    with conn:
        with conn.cursor() as curr:
            # Lists all paths related to json data files created in the first step.
            postgres_paths = [db_path for db_path in Path("data/postgres").glob(f"*/{user_date}/file.json")]
            csv_paths = [csv_path for csv_path in Path("data/csv").glob(f"*/{user_date}/file.json")]
            data_paths = postgres_paths + csv_paths

            # Loads and iterate over each data file of the first step.
            for data_path in data_paths:
                with open(data_path, "r", encoding="utf-8") as f:
                    json_data = json.load(f)
                
                # Get colunm and table name for convenience
                colunm_names = [colunm_info[0] for colunm_info in json_data.get("colunms_info")]
                table_name = json_data.get("table_name")
                
                # Before adding data to the table, the table is cleaned up of previous insertions. 
                curr.execute(sql.SQL(DELETE_TABLE_CONTENT).format(table=sql.Identifier(table_name)))

                # Creates query to insert data to the table using table and columns name.
                insert = sql.SQL(INSERT_INTO_TABLE).format(
                    table=sql.Identifier(table_name),
                    values=sql.SQL(", ").join(map(sql.Placeholder, colunm_names)))

                # Iterates over rows that were fetched from json file, and insert them in the database.
                for row in json_data.get("payload"):
                    curr.execute(insert, dict(zip(colunm_names, row)))
            
            # Selects orders and order_details tables by matching it's ids.
            curr.execute(SELECT_ALL_JOIN_ORDERS_ORDER_DETAILS)
            order_data = dict()
            # Iterate over each row and constructs a dictionary for each order containing it's info and it's multiple
            # details.
            ID = 0
            for row in curr.fetchall():
                try:
                    order_data[row[ID]]["details"].append(row[15:])
                except KeyError:
                    # KeyError will occur because of first occurrence of ID
                    # When fetched, row is a tuple.
                    mod_row = list(row)
                    # Colunms order_data, required_date and shipped_date are fetched as datetime objs. Since json format
                    # do not accept python obj, it's necessary to transform it to isoformat date strings.
                    for i in (3, 4, 5):
                        if mod_row[i] is not None:
                            mod_row[i] = mod_row[i].isoformat()
                    # Setting up the dictionary for the specific ID
                    order_data[mod_row[ID]] = dict()
                    order_data[mod_row[ID]]["order_info"] = mod_row[1:14]
                    order_data[mod_row[ID]]["details"] = list()
                    order_data[mod_row[ID]]["details"].append(mod_row[15:])

            # Create dir to store results
            path_to_store_results = Path("results/")
            path_to_store_results.mkdir(parents=True, exist_ok=True)
            
            # Saves order_data to json file named after the day that step2 was executed inside the result dir
            try:
                with open(path_to_store_results / f"{user_date}.json", "w", encoding="utf-8") as file:
                    json.dump(order_data, file, ensure_ascii=False, indent=4)
            except OSError as err:
                print(f"=> Error while trying to write query result to the filesystem: {err}")


if __name__ == "__main__":
    process_data_to_db("2021-12-20")
