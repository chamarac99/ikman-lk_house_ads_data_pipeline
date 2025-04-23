from bs4 import BeautifulSoup
import requests
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook 
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

def extract_ads_data(ti):
    url = 'https://ikman.lk/en/ads/dehiwala/houses-for-sale'
    html_text = requests.get(url).text

    ads = []

    soup = BeautifulSoup(html_text, 'lxml')
    house_ads = soup.find_all('li', class_ = ['top-ads-container--1Jeoq gtm-top-ad', 'normal--2QYVk gtm-normal-ad'])
    
    for house_ad in house_ads:
        ad_title = house_ad.find('h2', class_='heading--2eONR heading-2--1OnX8 title--3yncE block--3v-Ow').text
        no_of_rooms = house_ad.find("div", string=lambda text: text and "Bedrooms" in text).text.replace('Bedrooms: ','').replace('Bathrooms: ','').split(',')
        location = house_ad.find('div', class_='description--2-ez3').text.replace(', Houses For Sale','')
        price = house_ad.find('div', class_='price--3SnqI color--t0tGX').span.text

        ads.append({
            "Title": ad_title,
            "No_of_Bedrooms": no_of_rooms[0],
            "No_of_Bathrooms": no_of_rooms[1].strip(),
            "Location": location,
            "Price": price
        })


    df = pd.DataFrame(ads)
    df = df.drop_duplicates()
    
    adver = df.to_dict(orient='records')
    ti.xcom_push(key='ads_data', value=adver)

def insert_ads_into_postgres(ti):
    ad_data = ti.xcom_pull(key='ads_data', task_ids='extract_ads')

    postgres_hook = PostgresHook(postgres_conn_id='ikman_postgres')

    insert_query = """
    INSERT INTO ads (title, no_of_bedrooms, no_of_bathrooms, location, price)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (title, no_of_bedrooms, no_of_bathrooms, location, price) DO NOTHING;
    """
    
    for ad in ad_data:
        postgres_hook.run(insert_query, parameters=(ad['Title'], ad['No_of_Bedrooms'], ad['No_of_Bathrooms'], ad['Location'], ad['Price']))

default_args = {
    'owner': 'chamara',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_ikman_house_ads_V1.0',
    default_args=default_args,
    description='dag_with_ikman_house_ads',
    start_date=datetime(2025, 4, 7, 2),
    schedule_interval='@daily'
) as dag:

    extract_ads = PythonOperator(
        task_id='extract_ads',
        python_callable=extract_ads_data,
    )

    create_table_task = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='ikman_postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS ads (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                no_of_bedrooms TEXT,
                no_of_bathrooms TEXT,
                location TEXT,
                price TEXT,
                CONSTRAINT unique_ad UNIQUE (title, no_of_bedrooms, no_of_bathrooms, location, price)
            )
        """
    )

    insert_ads = PythonOperator(
        task_id='insert_ads',
        python_callable=insert_ads_into_postgres
    )

    extract_ads >> create_table_task >> insert_ads
