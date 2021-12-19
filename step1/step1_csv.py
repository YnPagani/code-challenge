import json
from pathlib import Path
import pandas as pd


def process_csv_data(user_date: str = None):
    """
    Reads data from csv and writes it to the filesystem.
    """
    # To maintain consistency with step1_db of how data is saved in file system, it was necessary to hardcode the data
    # type conversion.
    DATA_TYPES_CONVERSION = {
        "float64": "real",
        "int64": "smallint",
        "str": "character varying"
    }
    error_csv = False
    
    # Verifies if the csv exist.
    order_datails_csv = Path("data/order_details.csv")
    if order_datails_csv.is_file():
        table_data = dict()
        table_data["table_name"] = order_datails_csv.stem

        # Loads all data from csv.
        csv_dataframe = pd.read_csv(order_datails_csv)
        
        # For consistency reasons, column_info is created and composed of the column name and the column data type.
        csv_colunm_data_type = [csv_dataframe.dtypes[colunm_name] for colunm_name in csv_dataframe.head()]
        try:
            db_colunm_data_type = [DATA_TYPES_CONVERSION[data_type.name] for data_type in csv_colunm_data_type]
        except KeyError as err:
            error_csv = True
            print(f"=> Error when trying to convert csv colunm datatype to postgres datatype: {err} ")
        
        table_data["colunms_info"] = [colunm_info for colunm_info in zip(csv_dataframe.head(), db_colunm_data_type)]
        
        # Payload will consist of a list of all rows present in the csv.
        table_data["payload"] = list(csv_dataframe.itertuples(index=False, name=None))
        
        # If there were no errors during the whole process, table_data is send to be write in the file system.
        if not error_csv:
            error_write = _write_to_filesystem(table_data, user_date)
            return error_write
        else:
            return error_csv
        

def _write_to_filesystem(data: dict, user_date: str):
    """
    Helper function to write table content to the filesystem.
    """
    error_write = False
    table_name = data.get('table_name')
    
    # Create dir to store table content.
    path_to_store_table = Path(f"data/csv/{table_name}/{user_date}")
    path_to_store_table.mkdir(parents=True, exist_ok=True)
    
    # Saves table_content to json file.
    try:
        with open(path_to_store_table / "file.json", "w") as file:
            json.dump(data, file, indent=4)
        print(f"=> [{table_name}]: Table written to filesystem successfully")
    except OSError as err:
        error_write = True
        print(f"=> Error while trying to write [{table_name}] table  to the filesystem: {err}")
    
    return error_write


if __name__ == "__main__":
    pass
