from src.utils.utils import JSONHandler, DataFrameConsolidator, DataFrameConverter, CSVHandler, Path, DateTime
from etl.load.load import PostgresHandler, DataModeler
import os
from dotenv import load_dotenv
from datetime import datetime


env_path = '/opt/airflow/config/.env'
# Load environment variables from .env file
load_dotenv(env_path)

class TrackInfoExtractor:
    @staticmethod
    def extract_track_info(item, genre):
        track_info = {
            "track_name": item["name"],
            "popularity": item["popularity"],
            "track_id": item["id"],
            "duration_ms": item["duration_ms"],
            "album_name": item["album"]["name"],
            "release_date": item["album"]["release_date"],
            "total_tracks": item["album"]["total_tracks"],
            "artist_names": [artist["name"] for artist in item["artists"]],
            "artist_ids": [artist["id"] for artist in item["artists"]],
            "genre": genre  # Add genre column with genre name
        }
        return track_info

class TrackDataTransformer:
    def __init__(self, extractor):
        self.extractor = extractor

    def transform(self, data, genre):
        extracted_data = []
        for item in data:
            track_info = self.extractor.extract_track_info(item, genre)
            extracted_data.append(track_info)
        return extracted_data

class DataNotTransformed(Exception):
    pass

class Transform:
    def __init__(self, directory_path):
        self.directory_path = directory_path
        self.consolidator = DataFrameConsolidator()
        self.json_reader = JSONHandler()
        self.transformer = TrackDataTransformer(TrackInfoExtractor())
        self.converter = DataFrameConverter()

    def transform_data(self):
        for filename in os.listdir(self.directory_path):
            if filename.endswith(".json"):
                genre = filename.split('.')[0]
                file_path = os.path.join(self.directory_path, filename)
                data = self.json_reader.read_json_from_file(file_path)
                extracted_data = self.transformer.transform(data, genre)
                df = self.converter.to_dataframe(extracted_data)
                self.consolidator.add_dataframe(df)

        combined_df = self.consolidator.consolidate()
        combined_df['id'] = range(1, len(combined_df) + 1)
        # Reorder columns to have 'id' in the first position
        combined_df = combined_df[['id'] + [col for col in combined_df.columns if col != 'id']]
        return combined_df
    
        
if __name__ == "__main__":
    
    """
    This script uses the raw tracks data from staging directory and 
    transform and extract the relevant attributes from it. 
    Finally it store the relevant and transformed data in core layer.
    """
    layer = 'staging-layer'
    data_dir = '/opt/airflow/dags/data'

    
    try:
        today_path = Path.join_path(data_dir, layer,
                                         folder_name=DateTime.get_today_date())
        transform = Transform(today_path)
        combined_df = transform.transform_data()
        if not combined_df.empty:
            print("Data transformed.")
        else:
            raise DataNotTransformed("Data is not transformed.")
    except Exception as e:
        print("Error: ", e)
        
    if combined_df is not None:
        layer = 'core-layer'
        csv_writer = CSVHandler()
        file_name = 'spotify_tracks.csv'
        csv_writer.write_csv(
            combined_df, file_path=os.path.join(Path.make_dir(data_dir, layer, folder_name=DateTime.get_today_date()), file_name)
            )
        print("Data saved to core layer.")
        
        try:
            table_name = "spotify_tracks"
            schema = "core"
            mode = "replace"
            tables = []
            tables.append(combined_df)
            table_names = []
            table_names.append(table_name)
            
            postgres_handler = DataModeler(
                sql_directory='/opt/airflow/dags/sql/core-queries',
                user='postgres',
                password=os.getenv('POSTGRES_DB_PASSWORD'),
                host='localhost',
                port='5432',
                database='DataWarehouse'
            )
            postgres_handler.connect()
            postgres_handler.connection.set_session(autocommit=True)
            
            postgres_handler.create_tables()
                
            postgres_handler.upsert_data(
                tables, table_names=table_names, schema=schema)
            
        except Exception as e:
            print("Error: Unable to connect to the database.")
            print(e)

        finally:
            if postgres_handler:
                postgres_handler.disconnect()
        
    else:
        print("Failed to process data.")

            
