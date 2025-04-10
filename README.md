# 🏠 Ikman.lk House Ads Data Pipeline with Apache Airflow

<img src="assets/ikman_logo.png" alt="Ikman.lk Logo" width="800" height = "300"/>

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
- Python 3 <img src="https://www.python.org/static/community_logos/python-logo.png" alt="Python Logo" width="40"/>

---

## 📷 Architecture

<img src="assets/airflow_pipeline.png" alt="Pipeline Flow" width="300"/>

> **Data Flow**: Web Scraping → XCom Push → PostgreSQL Insert → DAG Scheduler

---

## ⚙️ Setup Instructions

### 🔧 Prerequisites

- Python ≥ 3.8
- PostgreSQL (local/cloud)
- Airflow environment (with CLI)

---

### 1. Clone the Repo

```bash
git clone https://github.com/chamarac99/ikman-house-ads-pipeline.git
cd ikman-house-ads-pipeline
```

### 2. Set Up Environment

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
### 3. Configure Airflow Connection
Use Airflow UI or CLI to create the connection:
```bash
airflow connections add 'ikman_postgres' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-login 'your_username' \
    --conn-password 'your_password' \
    --conn-schema 'your_dbname' \
    --conn-port '5432'
```
### 🗓️ Scheduling
-The DAG is configured to run daily (@daily) at 2 AM. You can modify it in the DAG file.

### 📦 Future Improvements
- Add support for scraping other cities.
- Store scraped data in cloud storage (e.g., AWS S3).
- Dashboard visualization using Streamlit or Metabase.

### 👨‍💻 Author
- Chamara Bandara
