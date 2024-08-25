from airflow.models import BaseOperator
from include.newsapi.hooks.NewsAPIHook import NewsAPIHook
import json


class GetNewsOperator(BaseOperator):
    def __init__(self, conn_id, search_keyword, *args, **kwargs):
        super().__init__(**kwargs)
        self._conn_id = conn_id
        self._search_keyword = search_keyword
        self.endpoint = "/api/1/news"

    def execute(self, context):
        news_api_hook = NewsAPIHook(self._conn_id)
        try:
            news_data = news_api_hook.call_news_api("/api/1/news", search_keyword=self._search_keyword)
            
            select_properties = [{key: item[key] for key in ('article_id','title','link','keywords','description','pubDate','country','category')} for item in news_data]
            select_properties = [{"id" if k == "article_id" else k:v for k,v in select_property.items()} for select_property in select_properties]
            
            with open('/home/astro/data.json', 'w') as file:
                json.dump(select_properties, file, indent=4)
        
        finally:
            news_api_hook.close()
