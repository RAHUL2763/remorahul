import oracledb
import pandas as pd
from sqlalchemy import create_engine, Float
from sqlalchemy.types import Integer, String
import tkinter as tk
from tkinter import messagebox

# Initialize Oracle Client
oracledb.init_oracle_client(lib_dir=r"C:\oracle\instantclient_23_5")  # Update with your Oracle Instant Client path

# Connection details
username = "SYSTEM"
password = "neoquant2018"
hostname = "192.168.1.245"
port = "1521"
sid = "xe"
dsn = f"{hostname}:{port}/{sid}"

def fetch_data_from_oracle():
    """Fetch data from Oracle database and return as a pandas DataFrame."""
    try:
        # Establish the connection
        connection = oracledb.connect(user=username, password=password, dsn=dsn)
        print("Successfully connected to Oracle Database")
        
        # Create a cursor object to interact with the database
        cursor = connection.cursor()

        # SQL query to fetch data from the DEMO_EMPLOYEES table
        query = "SELECT * FROM DEMO_EMPLOYEES"

        # Execute the query
        cursor.execute(query)

        # Fetch all results
        employees = cursor.fetchall()

        # Check if any data is returned
        if not employees:
            print("No data found in DEMO_EMPLOYEES table")
            return None

        # Get column names
        column_names = [desc[0] for desc in cursor.description]

        # Create a DataFrame from the fetched data
        df = pd.DataFrame(employees, columns=column_names)
        return df

    except oracledb.Error as e:
        print(f"Error fetching data from Oracle: {e}")
        return None

    finally:
        # Close the cursor and connection
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

def create_table_in_oracle():
    """Create a new table in Oracle to store deduplicated data if it doesn't exist."""
    try:
        # Establish connection to Oracle
        connection = oracledb.connect(user=username, password=password, dsn=dsn)
        cursor = connection.cursor()
        
        # SQL query to create a new table with the necessary columns
        create_table_query = """
        CREATE TABLE VINTAGE_EMPLOYEES (
            EMPLOYEE_ID NUMBER PRIMARY KEY,
            FIRST_NAME VARCHAR2(255),
            LAST_NAME VARCHAR2(255),
            EMAIL VARCHAR2(255),
            PHONE_NUMBER VARCHAR2(50),
            HIRE_DATE DATE,
            JOB_ID VARCHAR2(255),
            SALARY NUMBER,
            COMMISSION_PCT NUMBER,
            MANAGER_ID NUMBER,
            DEPARTMENT_ID NUMBER
        )
        """
        cursor.execute(create_table_query)
        print("Table 'VINTAGE_EMPLOYEES' created successfully.")

    except oracledb.Error as e:
        if "ORA-00955" in str(e):
            print("Table 'VINTAGE_EMPLOYEES' already exists.")
        else:
            print(f"Error creating table in Oracle: {e}")
    finally:
        # Close the cursor and connection
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

def deduplicate_data(df):
    """Deduplicate data based on the 'SALARY' column and return deduplicated DataFrame."""
    if df is not None:
        # Deduplication based on the 'SALARY' column
        deduplicated_df = df.drop_duplicates(subset=['SALARY'], keep='first')
        return deduplicated_df
    else:
        print("No data to deduplicate.")
        return None

def save_deduplicated_data_to_oracle(df, table_name='VINTAGE_EMPLOYEES'):
    """Save deduplicated data back to Oracle database."""
    try:
        if df is not None:
            # Create SQLAlchemy engine for Oracle
            engine = create_engine(f'oracle+oracledb://{username}:{password}@{dsn}')

            # Write the deduplicated DataFrame to the new table in Oracle
            df.to_sql(table_name, con=engine, if_exists='append', index=False)
            print(f"Deduplicated data stored in Oracle table '{table_name}'")
        else:
            print("No data to store in Oracle.")

    except Exception as e:
        print(f"Error saving data to Oracle: {e}")

def show_completion_popup():
    """Show a pop-up window to indicate the process is completed successfully."""
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    messagebox.showinfo("Process Completed", "All operations were completed successfully!")
    root.destroy()

def main():
    """Main function to run the workflow."""
    # Create the new Oracle table
    create_table_in_oracle()

    # Fetch data from Oracle
    df = fetch_data_from_oracle()

    if df is not None:
        print("Original DataFrame:")
        print(df)

        # Deduplicate the data
        deduplicated_df = deduplicate_data(df)
        
        if deduplicated_df is not None:
            print("\nDeduplicated DataFrame (based on Salary):")
            print(deduplicated_df)

            # Save deduplicated data back to Oracle
            save_deduplicated_data_to_oracle(deduplicated_df)

    print("\nProcess completed successfully!")  # Message indicating completion
    
    # Show a pop-up to indicate the process is completed
    show_completion_popup()

if __name__ == "__main__":
    main()
