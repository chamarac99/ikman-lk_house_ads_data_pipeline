{\rtf1\ansi\ansicpg1252\cocoartf2821
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fnil\fcharset0 Menlo-Regular;}
{\colortbl;\red255\green255\blue255;\red70\green137\blue204;\red23\green23\blue23;\red202\green202\blue202;
\red194\green126\blue101;\red167\green197\blue152;}
{\*\expandedcolortbl;;\cssrgb\c33725\c61176\c83922;\cssrgb\c11765\c11765\c11765;\cssrgb\c83137\c83137\c83137;
\cssrgb\c80784\c56863\c47059;\cssrgb\c70980\c80784\c65882;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\fs24 \cf2 \cb3 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 from\cf4 \strokec4  bs4 \cf2 \strokec2 import\cf4 \strokec4  BeautifulSoup\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 import\cf4 \strokec4  requests\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 import\cf4 \strokec4  pandas \cf2 \strokec2 as\cf4 \strokec4  pd\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 from\cf4 \strokec4  airflow.providers.postgres.hooks.postgres \cf2 \strokec2 import\cf4 \strokec4  PostgresHook \cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 from\cf4 \strokec4  airflow.providers.common.sql.operators.sql \cf2 \strokec2 import\cf4 \strokec4  SQLExecuteQueryOperator\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 from\cf4 \strokec4  airflow \cf2 \strokec2 import\cf4 \strokec4  DAG\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 from\cf4 \strokec4  datetime \cf2 \strokec2 import\cf4 \strokec4  datetime, timedelta\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 from\cf4 \strokec4  airflow.operators.python \cf2 \strokec2 import\cf4 \strokec4  PythonOperator\cf4 \cb1 \strokec4 \
\
\cf2 \cb3 \strokec2 def\cf4 \strokec4  extract_ads_data(ti):\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 \strokec4     url = \cf5 \strokec5 'https://ikman.lk/en/ads/dehiwala/houses-for-sale'\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     html_text = requests.get(url).text\cf4 \cb1 \strokec4 \
\
\cf4 \cb3 \strokec4     ads = []\cf4 \cb1 \strokec4 \
\
\cf4 \cb3 \strokec4     soup = BeautifulSoup(html_text, \cf5 \strokec5 'lxml'\cf4 \strokec4 )\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     house_ads = soup.find_all(\cf5 \strokec5 'li'\cf4 \strokec4 , class_ = [\cf5 \strokec5 'top-ads-container--1Jeoq gtm-top-ad'\cf4 \strokec4 , \cf5 \strokec5 'normal--2QYVk gtm-normal-ad'\cf4 \strokec4 ])\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     \cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     \cf2 \strokec2 for\cf4 \strokec4  house_ad \cf2 \strokec2 in\cf4 \strokec4  house_ads:\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4         ad_title = house_ad.find(\cf5 \strokec5 'h2'\cf4 \strokec4 , class_=\cf5 \strokec5 'heading--2eONR heading-2--1OnX8 title--3yncE block--3v-Ow'\cf4 \strokec4 ).text\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4         no_of_rooms = house_ad.find(\cf5 \strokec5 "div"\cf4 \strokec4 , string=\cf2 \strokec2 lambda\cf4 \strokec4  text: text \cf2 \strokec2 and\cf4 \strokec4  \cf5 \strokec5 "Bedrooms"\cf4 \strokec4  \cf2 \strokec2 in\cf4 \strokec4  text).text.replace(\cf5 \strokec5 'Bedrooms: '\cf4 \strokec4 ,\cf5 \strokec5 ''\cf4 \strokec4 ).replace(\cf5 \strokec5 'Bathrooms: '\cf4 \strokec4 ,\cf5 \strokec5 ''\cf4 \strokec4 ).split(\cf5 \strokec5 ','\cf4 \strokec4 )\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4         location = house_ad.find(\cf5 \strokec5 'div'\cf4 \strokec4 , class_=\cf5 \strokec5 'description--2-ez3'\cf4 \strokec4 ).text.replace(\cf5 \strokec5 ', Houses For Sale'\cf4 \strokec4 ,\cf5 \strokec5 ''\cf4 \strokec4 )\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4         price = house_ad.find(\cf5 \strokec5 'div'\cf4 \strokec4 , class_=\cf5 \strokec5 'price--3SnqI color--t0tGX'\cf4 \strokec4 ).span.text\cf4 \cb1 \strokec4 \
\
\cf4 \cb3 \strokec4         ads.append(\{\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4             \cf5 \strokec5 "Title"\cf4 \strokec4 : ad_title,\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4             \cf5 \strokec5 "No_of_Bedrooms"\cf4 \strokec4 : no_of_rooms[\cf6 \strokec6 0\cf4 \strokec4 ],\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4             \cf5 \strokec5 "No_of_Bathrooms"\cf4 \strokec4 : no_of_rooms[\cf6 \strokec6 1\cf4 \strokec4 ].strip(),\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4             \cf5 \strokec5 "Location"\cf4 \strokec4 : location,\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4             \cf5 \strokec5 "Price"\cf4 \strokec4 : price\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4         \})\cf4 \cb1 \strokec4 \
\
\
\cf4 \cb3 \strokec4     df = pd.DataFrame(ads)\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     df = df.drop_duplicates()\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     \cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     adver = df.to_dict(orient=\cf5 \strokec5 'records'\cf4 \strokec4 )\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     ti.xcom_push(key=\cf5 \strokec5 'ads_data'\cf4 \strokec4 , value=adver)\cf4 \cb1 \strokec4 \
\
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 def\cf4 \strokec4  insert_ads_into_postgres(ti):\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 \strokec4     ad_data = ti.xcom_pull(key=\cf5 \strokec5 'ads_data'\cf4 \strokec4 , task_ids=\cf5 \strokec5 'extract_ads'\cf4 \strokec4 )\cf4 \cb1 \strokec4 \
\
\cf4 \cb3 \strokec4     postgres_hook = PostgresHook(postgres_conn_id=\cf5 \strokec5 'ikman_postgres'\cf4 \strokec4 )\cf4 \cb1 \strokec4 \
\
\cf4 \cb3 \strokec4     insert_query = \cf5 \strokec5 """\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf5 \cb3 \strokec5     INSERT INTO ads (title, no_of_bedrooms, no_of_bathrooms, location, price)\cf4 \cb1 \strokec4 \
\cf5 \cb3 \strokec5     VALUES (%s, %s, %s, %s, %s)\cf4 \cb1 \strokec4 \
\cf5 \cb3 \strokec5     ON CONFLICT (title, no_of_bedrooms, no_of_bathrooms, location, price) DO NOTHING;\cf4 \cb1 \strokec4 \
\cf5 \cb3 \strokec5     """\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 \strokec4     \cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     \cf2 \strokec2 for\cf4 \strokec4  ad \cf2 \strokec2 in\cf4 \strokec4  ad_data:\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4         postgres_hook.run(insert_query, parameters=(ad[\cf5 \strokec5 'Title'\cf4 \strokec4 ], ad[\cf5 \strokec5 'No_of_Bedrooms'\cf4 \strokec4 ], ad[\cf5 \strokec5 'No_of_Bathrooms'\cf4 \strokec4 ], ad[\cf5 \strokec5 'Location'\cf4 \strokec4 ], ad[\cf5 \strokec5 'Price'\cf4 \strokec4 ]))\cf4 \cb1 \strokec4 \
\
\cf4 \cb3 \strokec4 default_args = \{\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     \cf5 \strokec5 'owner'\cf4 \strokec4 : \cf5 \strokec5 'chamara'\cf4 \strokec4 ,\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     \cf5 \strokec5 'retries'\cf4 \strokec4 : \cf6 \strokec6 5\cf4 \strokec4 ,\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     \cf5 \strokec5 'retry_delay'\cf4 \strokec4 : timedelta(minutes=\cf6 \strokec6 2\cf4 \strokec4 )\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4 \}\cf4 \cb1 \strokec4 \
\
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 with\cf4 \strokec4  DAG(\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 \strokec4     dag_id=\cf5 \strokec5 'dag_with_ikman_house_ads_V1.0'\cf4 \strokec4 ,\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     default_args=default_args,\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     description=\cf5 \strokec5 'dag_with_ikman_house_ads'\cf4 \strokec4 ,\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     start_date=datetime(\cf6 \strokec6 2025\cf4 \strokec4 , \cf6 \strokec6 4\cf4 \strokec4 , \cf6 \strokec6 7\cf4 \strokec4 , \cf6 \strokec6 2\cf4 \strokec4 ),\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     schedule_interval=\cf5 \strokec5 '@daily'\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4 ) \cf2 \strokec2 as\cf4 \strokec4  dag:\cf4 \cb1 \strokec4 \
\
\cf4 \cb3 \strokec4     extract_ads = PythonOperator(\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4         task_id=\cf5 \strokec5 'extract_ads'\cf4 \strokec4 ,\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4         python_callable=extract_ads_data,\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     )\cf4 \cb1 \strokec4 \
\
\cf4 \cb3 \strokec4     create_table_task = SQLExecuteQueryOperator(\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4         task_id=\cf5 \strokec5 'create_table'\cf4 \strokec4 ,\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4         conn_id=\cf5 \strokec5 'ikman_postgres'\cf4 \strokec4 ,\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4         sql=\cf5 \strokec5 """\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf5 \cb3 \strokec5             CREATE TABLE IF NOT EXISTS ads (\cf4 \cb1 \strokec4 \
\cf5 \cb3 \strokec5                 id SERIAL PRIMARY KEY,\cf4 \cb1 \strokec4 \
\cf5 \cb3 \strokec5                 title TEXT NOT NULL,\cf4 \cb1 \strokec4 \
\cf5 \cb3 \strokec5                 no_of_bedrooms TEXT,\cf4 \cb1 \strokec4 \
\cf5 \cb3 \strokec5                 no_of_bathrooms TEXT,\cf4 \cb1 \strokec4 \
\cf5 \cb3 \strokec5                 location TEXT,\cf4 \cb1 \strokec4 \
\cf5 \cb3 \strokec5                 price TEXT,\cf4 \cb1 \strokec4 \
\cf5 \cb3 \strokec5                 CONSTRAINT unique_ad UNIQUE (title, no_of_bedrooms, no_of_bathrooms, location, price)\cf4 \cb1 \strokec4 \
\cf5 \cb3 \strokec5             )\cf4 \cb1 \strokec4 \
\cf5 \cb3 \strokec5         """\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 \strokec4     )\cf4 \cb1 \strokec4 \
\
\cf4 \cb3 \strokec4     insert_ads = PythonOperator(\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4         task_id=\cf5 \strokec5 'insert_ads'\cf4 \strokec4 ,\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4         python_callable=insert_ads_into_postgres\cf4 \cb1 \strokec4 \
\cf4 \cb3 \strokec4     )\cf4 \cb1 \strokec4 \
\
\cf4 \cb3 \strokec4     extract_ads >> create_table_task >> insert_ads\cf4 \cb1 \strokec4 \
\
}