from dotenv import load_dotenv
from pathlib import Path
import os
import subprocess
import psycopg2
from boto3 import session
from botocore.client import Config
import datetime
import uuid
import requests

env_path = Path('..') / '.env'
load_dotenv(dotenv_path=env_path)

# Set DB Creds
HOST="127.0.0.1"
DB = os.getenv('MASTER_DB_DATABASE')
USER = os.getenv('MASTER_DB_USERNAME')
PASS = os.getenv('MASTER_DB_PASSWORD')

# Connect to DB
con = psycopg2.connect(database=DB, user=USER, password=PASS, host=HOST, port="5432")

# print("Database connected")
session = session.Session()
backup = 0
backupId = uuid.uuid4()

def send_mail(subject, text):
    domain = os.getenv('MAILGUN_DOMAIN')
    requests.post(
        "https://api.mailgun.net/v3/" + domain + "/messages",
        auth=("api", os.getenv('MAILGUN_API_KEY'),
        data={"from": "Backup Bot <bot@domain.com",
              "to": ["you@domain.com"],
              "subject": subject,
              "text": text})


def backup_db(db):

    fileName =  ''.join(db) + ".sql"

    os.system('pg_dump -U ' + USER + ' -F p ' + ''.join(db) + ' > ' + os.path.abspath("backups/" + fileName))

    print("Backing up..")
    
    # upload to do
    upload_to_do(fileName)

    print("Uploading...")

    # delete local file
    os.remove(os.path.abspath("backups/" + fileName))

    print("Deleted")

    # done
    print(''.join(db) + " Backed Up!")


def upload_to_do(filename):
    date = datetime.datetime.now()
    filePath = os.path.abspath("backups/" + filename)
    folder = "backups/db/" + str(os.getenv('APP_NAME')) + "/" + str(date.year) + "/" + str(date.month) + "/" + str(date.day) + str(backupId) + "/" + filename

    client = session.client('s3',
                        region_name=os.getenv('DO_SPACES_REGION'),
                        endpoint_url=os.getenv('DO_SPACES_ENDPOINT'),
                        aws_access_key_id=os.getenv('DO_SPACES_KEY'),
                        aws_secret_access_key=os.getenv('DO_SPACES_SECRET'))
    client.upload_file(filePath, os.getenv('DO_SPACES_BUCKET'), folder)

    return True


# Get all db
cur = con.cursor()
cur.execute('SELECT d.datname as "Name" FROM pg_catalog.pg_database as d ORDER BY 1')

rows = cur.fetchall()

for row in rows:
    backup_db(row)

con.close()

total_db = len(rows)
send_mail('Backup Complete', 'Backed up ' + str(total_db) + ' databases!')
