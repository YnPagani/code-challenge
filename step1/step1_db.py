import json
from datetime import date
from pathlib import Path
import psycopg2
from psycopg2 import sql

SELECT_ALL_TABLE_NAMES = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public';
    """
SELECT_ALL_COLUNM_NAMES_AND_DATATYPE = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = %s
    ORDER BY ordinal_position;
    """


def process_postgress_db(connection, user_date: str = None):
    with connection as conn:
        with conn.cursor() as curr:
            tables_names = None
            curr.execute(SELECT_ALL_TABLE_NAMES)
            
            tables_names = [table[0] for table in curr.fetchall()]
    
            for table in tables_names:
                table_data = dict()
                table_data["table_name"] = table
                
                curr.execute(SELECT_ALL_COLUNM_NAMES_AND_DATATYPE, (table,))
                table_data["colunms_info"] = curr.fetchall()

                index_to_convert = list()
                for i, colunm_info in enumerate(table_data["colunms_info"]):
                    _, colunm_datatype = colunm_info
                    if colunm_datatype in ('bytea', 'date'):
                        index_to_convert.append([i, colunm_datatype])
                    
                curr.execute(sql.SQL("SELECT * FROM {table}").format(table=sql.Identifier(table)))
                error_flag = False
                payload = list()
                while True:
                    try:
                        rows = curr.fetchmany(5000)
                        if not rows:
                            print(f"=> [{table_data['table_name']}]: Fetched all data from table sucssesfuly")
                            break
                        
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
                            for row in rows:
                                payload.append(row)
                        
                    except psycopg2.ProgrammingError as err:
                        error_flag = True
                        print(f"=> error when trying to fetch from {table}: {err}")
                
                if not error_flag:
                    table_data["payload"] = payload
                    _write_to_filesystem(table_data, user_date)


def _write_to_filesystem(data: dict, user_date: str):
    if user_date is not None:
        current_date = user_date
    else:
        current_date = date.today().isoformat()
    table_name = data.get('table_name')
    
    path_to_store_table = Path(f"data/postgres/{table_name}/{current_date}")
    path_to_store_table.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(path_to_store_table / "file.json", "w") as file:
            json.dump(data, file, indent=4)
    except OSError as err:
        print(f"=> Error while trying to write [{table_name}] table  to the filesystem: {err}")    
    
    print(f"=> [{table_name}]: Table written to filesystem successfully")
    

if __name__ == "__main__":
    process_postgress_db()
