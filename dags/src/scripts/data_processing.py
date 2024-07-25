import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from etl.load.load import PostgresHandler, DataModeler
from src.utils.utils import CSVHandler, Path, DateTime
import os
import io, re
from dotenv import load_dotenv
import ast

env_path = '/opt/airflow/config/.env'
# Load environment variables from .env file
load_dotenv(env_path)

class DataProcessor:
    def __init__(self, df):
        self.df = df
        self.db_handler = PostgresHandler(
            user='postgres',
            password=os.getenv('POSTGRES_DB_PASSWORD'),
            host='host.docker.internal',
            port='5432',
            database='DataWarehouse'
        )

    def normalize_data(self):
        # Convert string representations of lists to actual lists
        self.df['artist_names'] = self.df['artist_names'].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

        self.df['artist_ids'] = self.df['artist_ids'].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
        
        # Create album table
        self.albums = self.df[['album_name', 'release_date',
                               'total_tracks']].drop_duplicates().reset_index(drop=True)
        self.albums['album_id'] = range(1, len(self.albums) + 1)

        # Create artist table 
        self.artists_exploded = self.df[['id', 'artist_names', 'artist_ids']].explode(
            ['artist_names', 'artist_ids']).drop_duplicates().reset_index(drop=True)
        self.artists = self.artists_exploded[[
            'artist_names', 'artist_ids']].drop_duplicates().reset_index(drop=True)
        self.artists['artist_id'] = range(1, len(self.artists) + 1)

        self.tracks = self.df.merge(
            self.albums, on=['album_name', 'release_date', 'total_tracks'], how='left')
        
        self.track_artists = self.artists_exploded.merge(
            self.artists, on=['artist_names', 'artist_ids'], how='left')
        self.track_artists['uid'] = range(1, len(self.track_artists) + 1)
        
        # Create final dataframe of normalized tables
        self.track_artists = self.track_artists[['uid', 'id', 'artist_id']]
        self.tracks = self.tracks[[
            'id', 'track_name', 'popularity', 'track_id', 'duration_ms', 'genre', 'album_id']]
        self.albums = self.albums[[
            'album_id', 'album_name', 'release_date', 'total_tracks']]
        self.artists = self.artists[[
            'artist_id', 'artist_names', 'artist_ids']]

        self.albums['release_date'] = pd.to_datetime(
            self.albums['release_date'], format='%Y-%m-%d', errors='coerce')
        self.track_artists.rename(columns={'id': 'track_id','uid':'id'}, inplace=True)

        
    def create_bridge_table(self):
        pass

        
    def write_data(self, table_info):
        # Write normalized data to csv files
        csv_writer = CSVHandler()

        for df_name, info in table_info.items():
            # Extract table name and file path from the dictionary
            table = info['table']
            file_path = info['file_path']


            if table is not None:
                # Write dataframe to CSV file
                csv_writer.write_csv(table, file_path)
                print(f"{df_name.capitalize()} data written to {file_path}")
            else:
                print(f"Dataframe for {df_name} not found.")

    def get_dataframes(self):
        return self.albums, self.artists, self.tracks, self.track_artists


class DataReader:
    def __init__(self, database='postgres'):
        self.db_handler = PostgresHandler(
            user='postgres',
            password=os.getenv('POSTGRES_DB_PASSWORD'),
            host='host.docker.internal',
            port='5432',
            database=database
        )
        self.database = database
        
    def read_data_from_db(self, table, schema):
        try:
            self.db_handler.connect()
            self.db_handler.connection.set_session(autocommit=True)

            # Read data from database
            data, colnames = self.db_handler.fetch_data(table, schema)
            df = pd.DataFrame(data, columns=colnames).reset_index(drop=True)
            return df

        except Exception as e:
            print(f"Error: {e}")
            return None

        finally:
            if self.db_handler:
                self.db_handler.disconnect()
       

class DataReadingError(Exception):
    pass
                
# Example usage
if __name__ == "__main__":

    """ 
    Read data from core layer in database
    """

    data_reader = DataReader(database='DataWarehouse')
    data = data_reader.read_data_from_db(table='spotify_tracks_' + DateTime.get_today_date(), schema='core')
    if data.empty:
        raise DataReadingError("Data not read from core layer.")
    print("Data read from core layer.")

    """ 
    Normalize and process data in separate tables 
    """
    
    normalized_tables = []
    data_processor = DataProcessor(data)
    data_processor.normalize_data()
    normalized_tables.extend(data_processor.get_dataframes())

    """
    Write normalized table in csv files
    """
    # Assuming the order of dataframes in tables is: tracks, albums, artists, track_artists
    dataframe_names = ['albums', 'artists', 'tracks', 'track_artists']

    layer = 'consumption-layer'
    data_dir = '/opt/airflow/dags/data'
    
    # Define the base directory for saving CSV files
    base_file_path = Path.make_dir(data_dir, layer, folder_name=DateTime.get_today_date())

    # Create a dictionary of file paths based on dataframe names
    file_paths = {
        name: os.path.join(base_file_path, f"{name}.csv")
        for name in dataframe_names
    }
    
    table_info = {
        dataframe_names[0]: {
            'table': normalized_tables[0],
            'file_path': os.path.join(base_file_path, f"{dataframe_names[0]}.csv")
        },
        dataframe_names[1]: {
            'table': normalized_tables[1],
            'file_path': os.path.join(base_file_path, f"{dataframe_names[1]}.csv")
        },
        dataframe_names[2]: {
            'table': normalized_tables[2],
            'file_path': os.path.join(base_file_path, f"{dataframe_names[2]}.csv")
        },
        dataframe_names[3]: {
            'table': normalized_tables[3],
            'file_path': os.path.join(base_file_path, f"{dataframe_names[3]}.csv")
        }
    }
    
    data_processor.write_data(table_info)
   
    """ 
    Create data model and push data to database
    """
    try:
        # Database operations - schema creation
        schema = 'consumption'
        db_modeler = DataModeler(
            user='postgres',
            password=os.getenv('POSTGRES_DB_PASSWORD'),
            host='localhost',
            port='5432',
            database='DataWarehouse'
        )
        
        db_modeler.connect()
        db_modeler.connection.set_session(autocommit=True)
        
        db_modeler.create_tables()

        # Create a dictionary of dataframes
        dataframes = dict(zip(dataframe_names, normalized_tables))

        # Push each dataframe to its corresponding table
        db_modeler.upsert_data(
            normalized_tables, table_names=dataframe_names, schema=schema
            )
        
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        if db_modeler:
            db_modeler.disconnect()
