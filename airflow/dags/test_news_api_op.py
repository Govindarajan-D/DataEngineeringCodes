from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from include.newsapi.operators.GetNewsOperator import GetNewsOperator

from datetime import datetime

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3,},
    tags=["example"],
)
def test_news_api_op():
    get_news_data = GetNewsOperator(
        task_id="get_news_data_api",
        conn_id='news_data_API',
        search_keyword="S&P 500"
    )
    
    get_news_data

test_news_api_op()
