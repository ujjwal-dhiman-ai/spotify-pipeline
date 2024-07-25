from sqlalchemy import create_engine, Table, MetaData, inspect, text
import os
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
import psycopg2 
from psycopg2 import sql 
import io, os, re
from dotenv import load_dotenv

env_path = '/opt/airflow/config/.env'
# Load environment variables from .env file
load_dotenv(env_path)

class PostgresHandler:
    def __init__(self, **kwargs):
        self.connection_string = self.create_postgresql_connection_string(
            **kwargs)
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(self.connection_string)
            self.cursor = self.connection.cursor()
            print("Postgres connection established.")
        except psycopg2.Error as e:
            print("Error: ", e)

    def disconnect(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("Connection closed.")
            
    def create_postgresql_connection_string(self, **kwargs):
        user = kwargs.get('user', 'postgres')
        password = kwargs.get('password', 'password')
        host = kwargs.get('host', 'host.docker.internal')
        port = kwargs.get('port', '5432')
        database = kwargs.get('database', 'postgres')

        connection_string = f"postgresql://{user}:{
            password}@{host}:{port}/{database}"
        return connection_string

    def table_exists(self, table_name, schema='public'):
        try:
            # Check if the table exists in the specified schema
            query = """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    AND table_name = %s
                );
            """
            self.cursor.execute(query, (schema, table_name))
            return self.cursor.fetchone()[0]
        except psycopg2.Error as e:
            print(f"Error checking if table exists: {e}")
            return False

    def create_table(self, df, schema='public', table_name='your_table_name'):
        if self.table_exists(table_name=table_name, schema=schema):
            print(f"Table {table_name} already exists.")
            return
        
        type_mapping = {
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'object': 'TEXT',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'DATE',
        }

        # Adding a surrogate key
        columns_data_types = {
            'id': 'SERIAL PRIMARY KEY',  # Auto-incrementing integer
            **{
                column: type_mapping.get(str(df.dtypes[column]), 'TEXT')
                for column in df.columns
            }
        }

        table_creation_query = sql.SQL('''
            CREATE TABLE IF NOT EXISTS {}.{} (
                {}
            );
        ''').format(
            sql.Identifier(schema),
            sql.Identifier(table_name),
            sql.SQL(', ').join(sql.SQL('{} {}').format(sql.Identifier(column), sql.SQL(
                data_type)) for column, data_type in columns_data_types.items())
        )

        try:
            self.cursor.execute(table_creation_query)
            print(f"Table '{schema}.{table_name}' created successfully.")
        except Exception as e:
            print(f"Error creating table '{schema}.{table_name}': {e}")

    # def get_primary_key(self, schema, table_name):
    #     query = """
    #         SELECT kcu.column_name
    #         FROM information_schema.table_constraints tc
    #         JOIN information_schema.key_column_usage kcu
    #         ON tc.constraint_name = kcu.constraint_name
    #         AND tc.table_schema = kcu.table_schema
    #         WHERE tc.constraint_type = 'PRIMARY KEY' 
    #         AND tc.table_schema = %s 
    #         AND tc.table_name = %s;
    #     """
    #     self.cursor.execute(query, (schema, table_name))
    #     result = self.cursor.fetchone()
    #     if result:
    #         return result[0]
    #     else:
    #         raise ValueError(f"No primary key found for table {schema}.{table_name}")
    
    def get_primary_key(self, schema, table_name):
        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY' 
            AND tc.table_schema = %s 
            AND tc.table_name = %s;
        """
        self.cursor.execute(query, (schema, table_name))
        results = self.cursor.fetchall()
        if results:
            if len(results)== 1: return list(results[0])
            else: return [row[0] for row in results]
        else:
            raise ValueError(f"No primary key found for table {schema}.{table_name}")
            
    def fetch_last_id(self, schema, table_name):
        primary_key_column = []
        try:
            primary_key_column = self.get_primary_key(
                schema, table_name)
        except ValueError as e:
            print(e)
            return None

        last_id_query = f"SELECT MAX({primary_key_column[0]}) FROM {
            schema}.{table_name};"
        self.cursor.execute(last_id_query)
        last_id = self.cursor.fetchone()[0]

        # If the table is empty, set last_id to 0
        last_id = last_id if last_id is not None else 0

        return last_id, primary_key_column

    def update_id_column(self, df, last_id, primary_key_column):
        df[primary_key_column] = range(last_id + 1, last_id + 1 + len(df))
        return df

    def push_df_to_database(self, df, schema='public', table_name='your_table_name', mode='replace'):
        # Retrieve the last 'id' value from the existing table in the database if mode == 'append':
        if mode == 'append':
            last_id, primary_key_column = self.fetch_last_id(
                schema, table_name)
            df = self.update_id_column(df, last_id, primary_key_column)
        else:
            df = self.update_id_column(df, 0, primary_key_column)

        # Reorder columns to have 'id' in the first position
        # df = df[['id'] + [col for col in df.columns if col != 'id']]

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t')
        buffer.seek(0)

        if mode == 'append':
            copy_query = """
                COPY {}.{} FROM STDIN WITH CSV DELIMITER E'\\t' NULL AS '';
            """.format(schema, table_name)
        elif mode == 'replace':
            copy_query = """
                TRUNCATE TABLE {}.{};  -- This will delete all rows in the table
                COPY {}.{} FROM STDIN WITH CSV DELIMITER E'\\t' NULL AS '';
            """.format(schema, table_name, schema, table_name)
        else:
            raise ValueError(
                "Unsupported insertion mode. Use 'append' or 'replace'.")

        self.cursor.copy_expert(sql=copy_query, file=buffer)
        

    def fetch_data(self, table_name, schema, columns='*', condition=None):
        query = f"SELECT {columns} FROM {schema}.{table_name}"
        if condition:
            query += f" WHERE {condition}"

        try:
            self.cursor.execute(query)
            data = self.cursor.fetchall()
            colnames = [desc[0] for desc in self.cursor.description]
            return data, colnames
        except psycopg2.Error as e:
            print(f"Error fetching data: {e}")
            return None, None

    def execute_query(self, query, fetch_results=False):
        try:
            self.cursor.execute(query)
            if fetch_results:
                return self.cursor.fetchall()
        except Exception as e:
            print(f"Error executing query: {e}")


class DataModeler(PostgresHandler):      
    def __init__(self, sql_directory='/opt/airflow/dags/sql/schema-queries', **kwargs):
        super().__init__(**kwargs)
        self.sql_directory = sql_directory

    def extract_table_name_from_query(self, query):
        # Regular expression to match the schema and table name in a CREATE TABLE statement
        pattern = r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)\.(\w+)"

        match = re.search(pattern, query, re.IGNORECASE)

        if match:
            schema = match.group(1)
            table_name = match.group(2)
            return table_name, schema
        else:
            raise ValueError(
                "No schema and table name found in the provided SQL query.")

    def _read_sql_file(self, file_name):
        file_path = os.path.join(self.sql_directory, file_name)
        with open(file_path, 'r') as file:
            query = file.read().strip()  # Strip leading/trailing whitespace
        return query

    def _replace_table_name_in_query(self, query, new_table_name):
        """
        Replaces the table name in the SQL query with the provided new_table_name.

        Parameters:
        query (str): The SQL query to modify.
        new_table_name (str): The new table name to use in the query.

        Returns:
        str: The modified SQL query with the new table name.
        """
        # Regular expression to match the table name in CREATE TABLE statement
        pattern = re.compile(r'CREATE TABLE IF NOT EXISTS\s+(\w+)\.(\w+)\s*\(')
        match = pattern.search(query)

        if match:
            schema, old_table_name = match.groups()
            # Replace the old table name with the new table name
            new_query = pattern.sub(f'CREATE TABLE IF NOT EXISTS {schema}.{new_table_name} (', query)
            return new_query
        else:
            print("Table name not found in the SQL query.")
            return query

    def table_exists(self, table_name, schema='public'):
        try:
            # Check if the table exists in the specified schema
            query = """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    AND table_name = %s
                );
            """
            self.cursor.execute(query, (schema, table_name))
            return self.cursor.fetchone()[0]
        except psycopg2.Error as e:
            print(f"Error checking if table exists: {e}")
            return False

    def create_tables(self, new_table_name=None):
        # List all .sql files in the specified directory
        sql_files = [f for f in os.listdir(
            self.sql_directory) if f.endswith('.sql')]

        for sql_file in sql_files:
            query = self._read_sql_file(sql_file) 
            table_name, schema = self.extract_table_name_from_query(query)
            if new_table_name:
                # Replace table name in query if provided
                query = self._replace_table_name_in_query(query, new_table_name)
            
            if self.table_exists(table_name=table_name, schema=schema):
                print(f"Table {table_name} already exists.")
                continue
            self.cursor.execute(query)
            print(f"Table '{schema}.{table_name}' created successfully.")

    def insert_data(self, tables, table_names, schema='public'):
        # Define a dictionary with table names, corresponding dataframes, and COPY queries
        table_info = {
            'albums': {
                'data': tables[0],
                'query': f"""
                    COPY {schema}.{table_names[0]} (album_id, album_name, release_date, total_tracks)
                    FROM STDIN WITH CSV DELIMITER '\t' NULL AS ''
                """
            },
            'artists': {
                'data': tables[1],
                'query': f"""
                    COPY {schema}.{table_names[1]} (artist_id, artist_names, artist_ids)
                    FROM STDIN WITH CSV DELIMITER '\t' NULL AS ''
                """
            },
            'tracks': {
                'data': tables[2],
                'query': f"""
                    COPY {schema}.{table_names[2]} (id, track_name, popularity, track_id, duration_ms, album_id)
                    FROM STDIN WITH CSV DELIMITER '\t' NULL AS ''
                """
            },
            'track_artists': {
                'data': tables[3],
                'query': f"""
                    COPY {schema}.{table_names[3]} (track_id, artist_id)
                    FROM STDIN WITH CSV DELIMITER '\t' NULL AS ''
                """
            }
        }

        # Process each table
        for table_name, table in table_info.items():
            
            # Use this if statement if table exists with some data in it.
            if self.fetch_last_id(schema, table_name)[0] != 0:
                print(f"Table {table_name} already contains some data. Appending new data.")
                self.push_df_to_database(table['data'], schema=schema,table_name=table_name, mode='append')
                continue
            
            # Push the data to the database tables when they are empty
            else:
                # Create an in-memory buffer
                buffer = io.StringIO()
                table['data'].to_csv(buffer, index=False, header=False, sep='\t')
                buffer.seek(0)

                self.cursor.copy_expert(sql=table['query'], file=buffer)
                print(f"Data for table '{table_name}' pushed to database.")
                # Close the buffer
                buffer.close()

    # def upsert_data(self, tables, table_names, schema='public'):
    #     for df, table_name in zip(tables, table_names):
            
    #         try: 
    #             columns = df.columns.tolist()
    #             primary_key = self.get_primary_key_column(schema, table_name)
    #             if not primary_key:
    #                 raise ValueError(f"No primary key found for table {schema}.{table_name}")

    #             temp_table_name = f"{table_name}_temp"

    #             # Create a temporary table
    #             create_temp_query = f"""
    #             CREATE TEMP TABLE {temp_table_name} (LIKE {schema}.{table_name} INCLUDING ALL);
    #             """
    #             self.cursor.execute(create_temp_query)

    #             # Insert data into the temporary table
    #             buffer = io.StringIO()
    #             df.to_csv(buffer, index=False, header=False, sep='\t')
    #             buffer.seek(0)

    #             copy_query = f"""
    #             COPY {temp_table_name} ({', '.join(columns)})
    #             FROM STDIN WITH CSV DELIMITER '\t' NULL AS '';
    #             """
    #             self.cursor.copy_expert(sql=copy_query, file=buffer)
    #             buffer.close()

    #             update_columns = [col for col in columns if col != primary_key]
    #             update_set = ", ".join(
    #                 [f"{col} = EXCLUDED.{col}" for col in update_columns])

    #             # Upsert from the temporary table to the main table
    #             upsert_query = f"""
    #             INSERT INTO {schema}.{table_name} ({', '.join(columns)})
    #             SELECT * FROM {temp_table_name}
    #             ON CONFLICT ({primary_key}) DO UPDATE SET
    #             {update_set};
    #             """
    #             self.cursor.execute(upsert_query)

    #             # Drop the temporary table
    #             drop_temp_query = f"DROP TABLE {temp_table_name};"
    #             self.cursor.execute(drop_temp_query)
                
    #             print(f"Data updated/inserted in {table_name}.")
    #         except Exception as e:
    #             print(f"Error while upserting data for table {schema}.{table_name}: {e}")

    def upsert_data(self, tables, table_names, schema='public'):
        for df, table_name in zip(tables, table_names):
            try:
                columns = df.columns.tolist()
                primary_keys = self.get_primary_key(schema, table_name)
                if not primary_keys:
                    raise ValueError(f"No primary key found for table {schema}.{table_name}")

                primary_keys_str = ', '.join(primary_keys)
                temp_table_name = f"{table_name}_temp"

                # Create a temporary table
                create_temp_query = f"""
                CREATE TEMP TABLE {temp_table_name} (LIKE {schema}.{table_name} INCLUDING ALL);
                """
                self.cursor.execute(create_temp_query)

                # Insert data into the temporary table
                buffer = io.StringIO()
                df.to_csv(buffer, index=False, header=False, sep='\t')
                buffer.seek(0)

                copy_query = f"""
                COPY {temp_table_name} ({', '.join(columns)})
                FROM STDIN WITH CSV DELIMITER '\t' NULL AS '';
                """
                self.cursor.copy_expert(sql=copy_query, file=buffer)
                buffer.close()

                if self.is_bridge_table(table_name):
                    # Perform the delete-and-insert operation for bridge tables
                    delete_query = f"""
                    TRUNCATE TABLE {schema}.{table_name}
                    """
                    
                    self.cursor.execute(delete_query)

                    insert_query = f"""
                    INSERT INTO {schema}.{table_name} ({', '.join(columns)})
                    SELECT * FROM {temp_table_name};
                    """
                    self.cursor.execute(insert_query)
                else:
                    update_columns = [
                        col for col in columns if col not in primary_keys]
                    update_set = ", ".join(
                        [f"{col} = EXCLUDED.{col}" for col in update_columns])

                    # Upsert from the temporary table to the main table
                    if update_set:
                        upsert_query = f"""
                        INSERT INTO {schema}.{table_name} ({', '.join(columns)})
                        SELECT * FROM {temp_table_name}
                        ON CONFLICT ({primary_keys_str}) DO UPDATE SET
                        {update_set};
                        """
                    else:
                        upsert_query = f"""
                        INSERT INTO {schema}.{table_name} ({', '.join(columns)})
                        SELECT * FROM {temp_table_name}
                        ON CONFLICT ({primary_keys_str}) DO NOTHING;
                        """
                    self.cursor.execute(upsert_query)

                # Drop the temporary table
                drop_temp_query = f"DROP TABLE {temp_table_name};"
                self.cursor.execute(drop_temp_query)

                print(f"Data updated/inserted in {table_name}.")
            except Exception as e:
                print(f"Error while upserting data for table {schema}.{table_name}: {e}")

    def is_bridge_table(self, table_name):
            # Add logic to determine if a table is a bridge table
            # For demonstration purposes, using a hardcoded check
            bridge_tables = ['track_artists']  # Add other bridge table names if needed
            return table_name in bridge_tables

