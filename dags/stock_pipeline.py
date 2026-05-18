from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import psycopg2

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DB_CONN = {
    'host': 'postgres',
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'port': 5432
}

def create_table():
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS stock_prices (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(10),
            price FLOAT,
            volume BIGINT,
            date DATE,
            moving_avg_7d FLOAT,
            pct_change FLOAT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    ''')
    conn.commit()
    cur.close()
    conn.close()
    print("Table ready!")

def extract_stock_data():
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()
    stocks = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'NVDA', 'META', 'NFLX']
    for ticker in stocks:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='10d')
        price = hist['Close'].iloc[-1]
        volume = int(hist['Volume'].iloc[-1])
        date = hist.index[-1].date()
        moving_avg = hist['Close'].tail(7).mean()
        pct_change = ((hist['Close'].iloc[-1] - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2]) * 100
        cur.execute('''
            INSERT INTO stock_prices (ticker, price, volume, date, moving_avg_7d, pct_change)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (ticker, price, volume, date, moving_avg, pct_change))
        print(f"{ticker}: ${price:.2f} | 7d avg: ${moving_avg:.2f} | Change: {pct_change:.2f}%")
    conn.commit()
    cur.close()
    conn.close()

def check_alerts():
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()
    cur.execute('''
        SELECT ticker, price, pct_change 
        FROM stock_prices 
        WHERE created_at >= NOW() - INTERVAL '1 hour'
        AND ABS(pct_change) > 2
    ''')
    alerts = cur.fetchall()
    for ticker, price, pct in alerts:
        direction = "UP" if pct > 0 else "DOWN"
        print(f"ALERT: {ticker} is {direction} {pct:.2f}% - Current price: ${price:.2f}")
    if not alerts:
        print("No significant price movements detected.")
    cur.close()
    conn.close()

with DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Stock Data Warehouse Pipeline',
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    create = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    extract = PythonOperator(
        task_id='extract_stock_data',
        python_callable=extract_stock_data,
    )

    alerts = PythonOperator(
        task_id='check_alerts',
        python_callable=check_alerts,
    )

    create >> extract >> alerts
