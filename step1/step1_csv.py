import json
from pathlib import Path
from datetime import date
import pandas as pd


def process_csv_data(user_date: str = None):
    DATA_TYPES_CONVERSION = {
        "float64": "real",
        "int64": "smallint",
        "str": "character varying"
    }
    
    order_datails_csv = Path("data/order_details.csv")
    if order_datails_csv.is_file():
        table_data = dict()
        table_data["table_name"] = order_datails_csv.stem 
        csv_dataframe = pd.read_csv(order_datails_csv)
        
        csv_colunm_data_type = [csv_dataframe.dtypes[colunm_name] for colunm_name in csv_dataframe.head()]
        try:
            db_colunm_data_type = [DATA_TYPES_CONVERSION[data_type.name] for data_type in csv_colunm_data_type]
        except KeyError as err:
            print(f"=> Error when trying to convert csv colunm datatype to postgres datatype: {err} ")
        
        table_data["colunms_info"] = [colunm_info for colunm_info in zip(csv_dataframe.head(), db_colunm_data_type)]
        
        table_data["payload"] = list(csv_dataframe.itertuples(index=False, name=None))
        
        _write_to_filesystem(table_data, user_date)
        

def _write_to_filesystem(data: dict, user_date: str):
    if user_date is not None:
        current_date = user_date
    else:
        current_date = date.today().isoformat()
    table_name = data.get('table_name')
    
    path_to_store_table = Path(f"data/csv/{table_name}/{current_date}")
    path_to_store_table.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(path_to_store_table / "file.json", "w") as file:
            json.dump(data, file, indent=4)
    except OSError as err:
        print(f"=> Error while trying to write [{table_name}] table  to the filesystem: {err}")
    
    print(f"=> [{table_name}]: Table written to filesystem successfully")


if __name__ == "__main__":
    process_csv_data()