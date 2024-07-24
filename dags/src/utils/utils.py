import requests
from dotenv import load_dotenv
import os, json
import pandas as pd
import csv
from datetime import datetime

env_path = '/opt/airflow/config/.env'
# Load environment variables from .env file
load_dotenv(env_path)

class SpotifyAuthenticator:
    def __init__(self):
        load_dotenv()  # Load environment variables from .env file
        self.client_id = os.getenv('CLIENT_ID')
        self.client_secret = os.getenv('CLIENT_SECRET')
        self.token_url = "https://accounts.spotify.com/api/token"
        self.headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            # Add any additional headers here if needed
        }

    def get_access_token(self):
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }

        response = requests.post(
            self.token_url, headers=self.headers, data=payload)

        if response.status_code == 200:
            access_token = response.json().get('access_token')
            UpdateEnvironment.update_env_file(env_path, access_token)
            return access_token
        else:
            print(f"Failed to fetch access token. Status code: {response.status_code}")
            return None
        
class UpdateEnvironment:
    @staticmethod
    def update_env_file(env_path, access_token):
        # Remove surrounding quotes if present
        access_token = access_token.strip("'")

        # Read .env file
        with open(env_path, 'r') as file:
            lines = file.readlines()

        # Update or add ACCESS_TOKEN line
        with open(env_path, 'w') as file:
            found = False
            for line in lines:
                if line.startswith('ACCESS_TOKEN='):
                    file.write(f"ACCESS_TOKEN={access_token}\n")
                    found = True
                else:
                    file.write(line)
            if not found:
                file.write(f"\nACCESS_TOKEN={access_token}\n")

        print(f"Access Token updated in .env file.")

class JSONHandler:
    @staticmethod
    def read_json_from_file(file_path):
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    
    @staticmethod
    def dump_json_to_file(data, file_path):
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        # print(f"JSON data has been written to {file_path}")
 
class CSVHandler:
    @staticmethod
    def read_csv(file_path):
        """Reads a CSV file and returns a list of dictionaries."""
        data = []
        with open(file_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append(row)
        return data

    @staticmethod
    def write_csv(data, file_path=''):
        """Writes a list of dictionaries or a DataFrame to a CSV file without an index."""
        if isinstance(data, pd.DataFrame):
            if data.empty:
                raise ValueError("DataFrame is empty")
            data.to_csv(file_path, index=False)
        elif isinstance(data, list):
            if not data:
                
                raise ValueError("Data is empty or None")
            with open(file_path, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
        else:
            raise TypeError(
                "Data must be a pandas DataFrame or a list of dictionaries")
        
class DataFrameConsolidator:
    def __init__(self):
        self.all_dfs = []

    def add_dataframe(self, df):
        self.all_dfs.append(df)

    def consolidate(self):
        if not self.all_dfs:
            return None
        combined_df = pd.concat(self.all_dfs, ignore_index=True)
        return combined_df

class DataFrameConverter:
    @staticmethod
    def to_dataframe(data):
        return pd.DataFrame(data)

class Path:
    @staticmethod
    def join_path(base_path, layer, folder_name=''):
        folder_path = os.path.join(base_path, layer, folder_name)
        if os.path.isdir(folder_path):
            return folder_path
        else:
            return None

    @staticmethod
    def make_dir(base_path, layer, folder_name=''):
        new_folder_path = os.path.join(base_path, layer, folder_name)
        os.makedirs(new_folder_path, exist_ok=True)

        if os.path.exists(new_folder_path):
            return new_folder_path
        else:
            return None
        
class DateTime:
    @staticmethod
    def get_today_date():
        return datetime.today().strftime('%Y_%m_%d')
    
# Example usage:
if __name__ == "__main__":
    spotify_auth = SpotifyAuthenticator()
    access_token = spotify_auth.get_access_token()

    if access_token:
        print(f"Access token generated: {access_token}")
    else:
        print("Failed to generate access token.")
