from datetime import date
import psycopg2
from step1 import step1_db
from step1 import step1_csv
from step2 import step2


# Dictionary to store if a step was run or not and if it was sucssesful.
STEPS_EXECTUION = dict()


def _ask_for_date_isoformat():
    """
    Helper function to verify if date is in isoformat.
    """
    error = False
    current_day = date.today().isoformat()
    ASK_DATE = f"\nWhat day would you like to run this step? Date must be in yyyy-mm-dd format. (Default {current_day}): "
    iso_date = input(ASK_DATE)
    
    if iso_date == "":
        return error, current_day
    
    try:
        iso_date = date.fromisoformat(iso_date)
    except ValueError:
        error = True
    
    return error, iso_date
    

def run_step1_postgres():
    """
    Interface to run step1 related to postgres database.
    """
    error_date, iso_date = _ask_for_date_isoformat()
    if not error_date:
        error_conn = False
        try:
            step1_conn = psycopg2.connect(dbname="northwind", user="northwind_user", password="thewindisblowing")
        except psycopg2.Error as err:
            error_conn = True
            print(f"=> Error trying to connect to postgres database: {err}")
        
        if not error_conn:
            error_step1 = step1_db.process_postgress_db(step1_conn, user_date=iso_date)
            
            if not error_step1:
                try:
                    STEPS_EXECTUION[iso_date]["step1_db"] = True
                except KeyError:
                    STEPS_EXECTUION[iso_date] = dict()
                    STEPS_EXECTUION[iso_date]["step1_db"] = True        
    else:
        print("\nInvalid date, try again.")
    
    print(STEPS_EXECTUION)


def run_step1_csv():
    """
    Interface to run step1 related to csv table.
    """
    error_date, iso_date = _ask_for_date_isoformat()
    if not error_date:    
        error_step1 = step1_csv.process_csv_data(user_date=iso_date)
        if not error_step1:
            try:
                STEPS_EXECTUION[iso_date]["step1_csv"] = True
            except KeyError:
                STEPS_EXECTUION[iso_date] = dict()
                STEPS_EXECTUION[iso_date]["step1_csv"] = True
    else:
        print("\nInvalid date, try again.")            
    
    print(STEPS_EXECTUION)


def run_step2():
    """
    Interface to run step2.
    """
    error_date, iso_date = _ask_for_date_isoformat()
    if not error_date:
        try:
            if STEPS_EXECTUION[iso_date]["step1_db"] and STEPS_EXECTUION[iso_date]["step1_csv"]:
                error_conn = False
                try:
                    step2_conn = psycopg2.connect(dbname="solution", user="solution_user", password="hereitis", port=5438)
                except psycopg2.Error as err:
                    error_conn = True
                    print(f"=> Error trying to connect to postgres database: {err}")
                
                if not error_conn:
                    step2.process_data_to_db(conn=step2_conn, user_date=iso_date)
                    print("Result were saved in the folder results/")
            else:
                print(f"\nfor date {iso_date}, one or more steps have failed to execute. Please consider running the \
                      steps 1 again before trying step 2")
        except KeyError:
            print("One or more steps were not performed. Please consider running the steps 1 again before trying step 2")
    else:
        print("\nInvalid date, try again.")            

    
MENU_PROMPT = """

--- MENU ---
1) Run STEP1 Postgres.
2) Run STEP1 CSV.
3) Run STEP2.
4) Exit.

Enter your option: """


OPTIONS = {
    "1": run_step1_postgres,
    "2": run_step1_csv,
    "3": run_step2,
}

if __name__ == "__main__":
    while(True):
        selection = input(MENU_PROMPT)
        if selection == "4":
            break
        try:
            OPTIONS[selection]()
        except KeyError:
            print("\nInvalid option, try again.")
