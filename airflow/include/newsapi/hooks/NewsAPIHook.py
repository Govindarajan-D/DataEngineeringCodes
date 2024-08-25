from typing import Any
import requests
import json

from airflow.hooks.base import BaseHook

class NewsAPIHook(BaseHook):
    def __init__(self, conn_id: str):
        super().__init__()
        self.conn_id = conn_id
        self._get_resource()
    
    def _get_resource(self):
        self.connection = self.get_connection(self.conn_id)
        self.base_uri = f"{self.connection.schema}{self.connection.host}"
        self.password = self.connection.password

    def call_news_api(self, endpoint, **kwargs):
        call_url = f"{self.base_uri}{endpoint}"
        params = {'apikey':self.password,'qInTitle': kwargs.get("search_keyword")}

        try:
            news_response = requests.get(call_url,params=params)

            if news_response.ok:
                response_content = json.loads(news_response.content)
                news_data = response_content['results']
                return news_data
            
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching news: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON response: {str(e)}")
            raise
        
    
    def close(self):
        print("No objects to release")
