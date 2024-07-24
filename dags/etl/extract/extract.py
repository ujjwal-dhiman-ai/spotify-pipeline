import requests
import os
from dotenv import load_dotenv, set_key
from src.utils.utils import JSONHandler, Path, DateTime
import concurrent.futures
from datetime import datetime


env_path = '/opt/airflow/config/.env'
# Load environment variables from .env file
load_dotenv(env_path)

class Search:
    def __init__(self, access_token):
        self.base_url = "https://api.spotify.com/v1/"
        self.access_token = access_token
        self.headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

    def search_tracks(self, query, limit=50):
        url = f"{self.base_url}search"
        params = {
            'q': query,
            'type': 'track',
            'limit': limit
        }

        response = requests.get(url, headers=self.headers, params=params)
        return response.json() if response.status_code == 200 else None
    
    def next_search(next_url):
        response = requests.get(next_url)
        return response.json() if response.status_code == 200 else None

class TrackSearcher:
    def __init__(self, access_token):
        self.access_token = access_token
        self.base_url = "https://api.spotify.com/v1/"
        self.headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

    def search_tracks(self, query, limit=50):
        url = f"{self.base_url}search"
        params = {
            'q': query,
            'type': 'track',
            'limit': limit
        }

        response = requests.get(url, headers=self.headers, params=params)
        return response.json() if response.status_code == 200 else None

    def fetch_all_tracks(self, query, limit=50):
        tracks = []
        next_url = None
        initial_response = self.search_tracks(query, limit)

        if initial_response:
            tracks.extend(initial_response['tracks']['items'])
            next_url = initial_response['tracks'].get('next')

        while next_url:
            response = requests.get(next_url, headers=self.headers)
            if response.status_code != 200:
                break
            data = response.json()
            tracks.extend(data['tracks']['items'])
            next_url = data['tracks'].get('next')

        return tracks

class Extract:
    def __init__(self):
        self.access_token = os.getenv('ACCESS_TOKEN')
        self.search = TrackSearcher(self.access_token)

    def fetch_and_save_genre(self, genre, raw_data_path):
        query = f'year:2000-2024 genre:{genre}'
        limit = 50
        tracks = self.search.fetch_all_tracks(query=query, limit=limit)
        if tracks:
            json_file_path = os.path.join(raw_data_path, f"{genre}.json")
            JSONHandler.dump_json_to_file(tracks, json_file_path)
            print(f"Saved data for genre '{genre}' to {json_file_path}")
        else:
            print(f"Failed to fetch search results for genre '{genre}'.")
                
    def fetch_and_save_data(self, genre_list, raw_data_path):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(
                self.fetch_and_save_genre, genre, raw_data_path) for genre in genre_list]
            for future in concurrent.futures.as_completed(futures):
                future.result()

    @staticmethod
    def make_dir(base_path, folder_name=''):
        new_folder_path = os.path.join(base_path, folder_name)
        os.makedirs(new_folder_path, exist_ok=True)

        if os.path.exists(new_folder_path):
            return new_folder_path
        else:
            return None

    @staticmethod
    def get_today_date():
        return datetime.today().strftime('%Y_%m_%d')

# Example usage:
if __name__ == "__main__":
    
    """
    This script extract spotify tracks for every genre from their Web API, 
    and store them as json files in staging directory.
    """

    genre_list_path = r"D:\Data-Engineer\de-projects\spotify\dags\src\utils\genre.json"
    
    genre_list = JSONHandler.read_json_from_file(genre_list_path)
    genres = genre_list['genres']
    
    layer = 'staging-layer'
    data_dir = '/opt/airflow/dags/data'
    
    raw_data_path = Path.make_dir(
        data_dir, layer, folder_name=DateTime.get_today_date())
    
    extractor = Extract()
    extractor.fetch_and_save_data(genres, raw_data_path)