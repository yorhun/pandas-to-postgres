import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from io import StringIO
import numpy as np

def get_postgres_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    else:
        return 'TEXT'

def create_table_query(df, table_name):
    columns = []
    for column, dtype in df.dtypes.items():
        pg_type = get_postgres_type(dtype)
        columns.append(f'"{column}" {pg_type}')
    
    columns_string = ', '.join(columns)
    return f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, {columns_string})"

def load_csv_to_postgres(csv_file_path, table_name, db_connection_string, chunk_size=50000):
    # Create a database connection
    engine = create_engine(db_connection_string)
    conn = engine.raw_connection()
    cursor = conn.cursor()

    # Read the first chunk to get the schema
    first_chunk = next(pd.read_csv(csv_file_path, chunksize=1))
    
    # Create the table based on the DataFrame schema
    create_table_sql = create_table_query(first_chunk, table_name)
    cursor.execute(create_table_sql)
    conn.commit()

    # Read and insert the CSV in chunks
    for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size):
        # Replace NaN values with None for proper SQL NULL handling
        chunk = chunk.replace({np.nan: None})
        
        # Prepare the data for insertion
        output = StringIO()
        chunk.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)

        # Use COPY to efficiently insert the data
        cursor.copy_from(output, table_name, null="", columns=chunk.columns)
        conn.commit()

    # Close the connection
    cursor.close()
    conn.close()

    print(f"CSV file loaded into {table_name} successfully!")

if __name__ == "__main__":
    csv_file_path = "/app/data/csvfile.csv"
    table_name = "your_table_name"
    db_connection_string = "postgresql://myuser:mypassword@postgres:5432/mydb"

    load_csv_to_postgres(csv_file_path, table_name, db_connection_string)