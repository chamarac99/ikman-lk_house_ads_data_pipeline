# 🏠 Ikman.lk House Ads Data Pipeline with Apache Airflow

![Ikman.lk Logo](assets/ikman_logo.png)

This project automates the extraction of real estate advertisements from [ikman.lk](https://ikman.lk) for houses in Dehiwala using a web scraper. The data is then inserted into a PostgreSQL database through an **Apache Airflow** pipeline.

---

## 🚀 Features

- Extracts real-time ads from [ikman.lk](https://ikman.lk/en/ads/dehiwala/houses-for-sale)
- Parses data with BeautifulSoup (title, bedrooms, bathrooms, location, price)
- Inserts cleaned data into a PostgreSQL table
- Daily scheduled DAG using Apache Airflow
- Handles duplicate entries gracefully using `ON CONFLICT`

---

## 🛠️ Technologies Used

- [Apache Airflow](https://airflow.apache.org/)
- [PostgreSQL](https://www.postgresql.org/)
- [BeautifulSoup4](https://www.crummy.com/software/BeautifulSoup/)
- [Pandas](https://pandas.pydata.org/)
- Python 3

---

## 📷 Architecture

![Pipeline Flow](assets/airflow_pipeline.png)

> **Data Flow**: Web Scraping → XCom Push → PostgreSQL Insert → DAG Scheduler

---

## 📁 Project Structure

```bash
ikman-house-ads-pipeline/
├── dags/
│   └── dag_with_ikman_house_ads.py   # Airflow DAG script
├── README.md                         # You're reading this
├── requirements.txt                  # All dependencies
└── assets/
    ├── airflow_pipeline.png          # Architecture diagram
    └── ikman_logo.png                # Ikman.lk or custom project logo
