import json
from pathlib import Path
import psycopg2
from psycopg2 import sql

SELECT_ALL_TABLE_NAMES = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public';
    """

SELECT_ALL_COLUMN_NAMES_AND_DATATYPE = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = %s
    ORDER BY ordinal_position;
    """

SELECT_ALL_FROM_TABLE = """
    SELECT * 
    FROM {table};
    """


def process_postgress_db(conn, user_date: str):
    """
    Reads all content from northwind db and writes it to the filesystem.
    """
    with conn:
        with conn.cursor() as curr:
            # Fetch all table names from northwind db.
            curr.execute(SELECT_ALL_TABLE_NAMES)
            tables_names = [table[0] for table in curr.fetchall()]
            
            # Iterate over each table name to extract data.
            for table in tables_names:
                table_data = dict()
                table_data["table_name"] = table

                # For each column, fetch it's name and data type.
                curr.execute(SELECT_ALL_COLUMN_NAMES_AND_DATATYPE, (table,))
                table_data["colunms_info"] = curr.fetchall()

                # When using psycopg2 to fetch data from postgres db, the module converts data types like "date" and 
                # "bytea" to datetime obj and memoryview obj. Since json format does not accept python obj, it's 
                # necessary to convert this objs to primitive types.
                index_to_convert = list()
                for i, column_info in enumerate(table_data["colunms_info"]):
                    _, column_datatype = column_info
                    if column_datatype in ('bytea', 'date'):
                        index_to_convert.append([i, column_datatype])
                
                # Executes query to fetch all content from table.
                curr.execute(sql.SQL(SELECT_ALL_FROM_TABLE).format(table=sql.Identifier(table)))
                error_fetch = False
                payload = list()
                
                # Fetch rows till the end of table content.
                while True:
                    try:
                        rows = curr.fetchmany(5000)
                        # If there are no more rows to fetch, it has reached the end of the table.
                        if not rows:
                            print(f"=> [{table_data['table_name']}]: Fetched all data from table sucssesfuly")
                            break
                        # For tables that have "bytea" or "date" as it's columns data types. It's done data type 
                        # conversion and the row is appended to the payload.
                        if index_to_convert:
                            for row in rows:
                                row = list(row)
                                for i, datatype in index_to_convert:
                                    if row[i] is not None:
                                        if datatype == 'bytea':
                                            row[i] = row[i].tobytes().decode('UTF-8')
                                        elif datatype == 'date':
                                            row[i] = row[i].isoformat()

                                payload.append(tuple(row))
                        else:
                            # For tables with only primitive datat ypes, it's rows are appended to the payload as it was
                            # fetched.
                            for row in rows:
                                payload.append(row)
                        
                    except psycopg2.ProgrammingError as err:
                        error_fetch = True
                        print(f"=> error when trying to fetch from {table}: {err}")
                
                # If there were no errors during the whole process, table_data is send to be write in the file system.
                if not error_fetch:
                    table_data["payload"] = payload
                    error_write = _write_to_filesystem(table_data, user_date)
                    if error_write: 
                        return
                else:
                    return error_fetch


def _write_to_filesystem(data: dict, user_date: str):
    """
    Helper function to write table content to the filesystem.
    """
    error_write = False
    table_name = data.get('table_name')
    
    # Create dir to store table content.
    path_to_store_table = Path(f"data/postgres/{table_name}/{user_date}")
    path_to_store_table.mkdir(parents=True, exist_ok=True)
    
    # Saves table_content to json file.
    try:
        with open(path_to_store_table / "file.json", "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        print(f"=> [{table_name}]: Table written to filesystem successfully")
    except OSError as err:
        error_write = True
        print(f"=> Error while trying to write [{table_name}] table  to the filesystem: {err}")    
    
    return error_write


if __name__ == "__main__":
    pass

