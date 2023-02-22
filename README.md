# learningit
import bs4
from bs4 import BeautifulSoup
import urllib.request, urllib.parse, urllib.error
import ssl
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import sqlite3

def fun():
    ctx  = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    url_link = "https://www.hindustantimes.com"
    html = urllib.request.urlopen(url_link,context=ctx).read()
    soup = BeautifulSoup(html,'html.parser')
    tags = soup.find_all("a")

    conn = sqlite3.connect("/home/vboxuser/newair/mydb")
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS datascrape')
    cur.execute('CREATE TABLE datascrape(sno INT AUTO_INCREMENT, scrape TEXT)')

    for tag in tags:
        if isinstance(tag,bs4.element.Tag):
            if tag.has_attr("data-articleurl") and tag.has_attr("data-id"):
                at = tag.get("data-articleurl")
                cur.execute('INSERT INTO datascrape(scrape) VALUES ("{0}")'.format(at))
    conn.commit()
    cur.close()
    conn.close()

def fun1():
    conn = sqlite3.connect("mydb")
    cur = conn.cursor()
    res = cur.execute('SELECT scrape FROM datascrape')
    a = res.fetchall()
    fh = open("/home/vboxuser/newair/file1.txt","a")

    for k in a:
        for p in k:
            fh.write(p)
            fh.write("\n")
    fh.close()
    cur.close()
    conn.close()



default_args = {
    'owner': 'kartik',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='first_dag_v2',
    default_args=default_args,
    description='This is my first dag',
    start_date=datetime(2023,2,1),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='first_task',
        python_callable=fun
    )

    task2 = PythonOperator(
        task_id='second_task',
        python_callable=fun1
    )

    task1 >> task2
