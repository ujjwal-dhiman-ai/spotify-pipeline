from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

from src.utils.utils import SpotifyAuthenticator, JSONHandler, CSVHandler, Path, DateTime
from src.scripts.data_processing import DataProcessor, DataReader, DataReadingError
from etl.transform.transform import Transform, DataNotTransformed
from etl.extract.extract import Extract
from etl.load.load import DataModeler

from dotenv import load_dotenv

env_path = '/opt/airflow/config/.env'
# Load environment variables from .env file
load_dotenv(env_path)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'spotify_etl',
    default_args=default_args,
    description='A simple ETL process for Spotify data',
    schedule_interval=timedelta(minutes=20),
    catchup=False  # Ensure the DAG does not backfill missing runs
)

def authenticator():
    spotify_auth = SpotifyAuthenticator()
    access_token = spotify_auth.get_access_token()

    if access_token:
        print(f"Access token generated: {access_token}")
    else:
        print("Failed to generate access token.")
        
def extractor():
    genre_list_path = '/opt/airflow/dags/src/utils/genre.json'

    genre_list = JSONHandler.read_json_from_file(genre_list_path)
    genres = genre_list['genres']

    layer = 'staging-layer'
    data_dir = "D:\\Data-Engineer\\de-projects\\spotify\\dags\\data"

    raw_data_path = Path.make_dir(
        data_dir, layer, folder_name=DateTime.get_today_date())

    extractor = Extract()
    extractor.fetch_and_save_data(genres, raw_data_path)

def transformer():
    layer = 'staging-layer'
    data_dir = "D:\\Data-Engineer\\de-projects\\spotify\\dags\\data"

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

    
    layer = 'core-layer'
    csv_writer = CSVHandler()
    file_name = 'spotify_tracks.csv'
    csv_writer.write_csv(
        combined_df, file_path=os.path.join(Path.make_dir(
            data_dir, layer, folder_name=DateTime.get_today_date()), file_name)
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
            sql_directory=r'D:\Data-Engineer\de-projects\spotify\dags\sql\core-queries',
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
        print(f"Error: {e}")

    finally:
        if postgres_handler:
            postgres_handler.disconnect()

def data_processor():
    """ 
    Read data from core layer in database
    """

    data_reader = DataReader(database='DataWarehouse')
    data = data_reader.read_data_from_db(
        table='spotify_tracks_' + DateTime.get_today_date(), schema='core')
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
    data_dir = "D:\\Data-Engineer\\de-projects\\spotify\\dags\\data"

    # Define the base directory for saving CSV files
    base_file_path = Path.make_dir(
        data_dir, layer, folder_name=DateTime.get_today_date())

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
            'table': normalized_tables[0],
            'file_path': os.path.join(base_file_path, f"{dataframe_names[1]}.csv")
        },
        dataframe_names[2]: {
            'table': normalized_tables[0],
            'file_path': os.path.join(base_file_path, f"{dataframe_names[2]}.csv")
        },
        dataframe_names[3]: {
            'table': normalized_tables[0],
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

    
# Define the tasks
get_access_token = PythonOperator(
    task_id='get_access_token',
    python_callable=authenticator,
    dag=dag
)

extract = PythonOperator(
    task_id='extract_data',
    python_callable=extractor,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transformer,
    dag=dag,
)

process_data = PythonOperator(
    task_id='data_processing',
    python_callable=data_processor,
    dag=dag,
)

# Set the task dependencies
get_access_token >> extract >> transform >> process_data
