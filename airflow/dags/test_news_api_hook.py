from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from include.newsapi.hooks.NewsAPIHook import NewsAPIHook

from datetime import datetime

def call_news_api():
    news_hook = NewsAPIHook('news_data_API')
    news_hook._get_resource()

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def test_news_api_hook():
    run_news_hook = PythonOperator(
        task_id="Run_News_Hook",
        python_callable=call_news_api
    )
    
    run_news_hook

test_news_api_hook()
