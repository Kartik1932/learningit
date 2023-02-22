# learningit
Learning Git
import bs4
from bs4 import BeautifulSoup
import urllib.request, urllib.parse, urllib.error
import ssl
import requests

from datetime import datetime
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

    conn = sqlite3.connect("mydb")
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS datascrape')
    cur.execute('CREATE TABLE datascrape(sno INT AUTO_INCREMENT, scrape TEXT)')

    for tag in tags:
        if isinstance(tag,bs4.element.Tag):
            if tag.has_attr("data-articleurl") and tag.has_attr("data-id"):
                # print(tag.get("data-articleurl"))
                at = tag.get("data-articleurl")
                cur.execute('INSERT INTO datascrape(scrape) VALUES ("{0}")'.format(at))

dag = DAG(
    'example_dag',
    default_args={'owner':'airflow',
                    'startdate':datetime(2023,3,4)}
)

with dag:
    task1 = PythonOperator(
        task_id='fun',
        python_callable=fun
    )
