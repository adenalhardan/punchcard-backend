import random
import string
import json

from fastapi import FastAPI
from mangum import Mangum
import boto3

params = json.load(open('params.json'))

app = FastAPI()
handler = Mangum(app)

rds_client = boto3.client('rds-data')

def execute(sql):
    response = rds_client.execute_statement(
        secretArn = params['database_credentials_secret_store_arn'],
        database = params['database_name'],
        resourceArn = params['database_cluster_arn'],
        sql = sql
    )

    return json.loads(response['records'])

@app.get('/')
async def root():
    return {'message': 'its all good'}

@app.get('/get-prefix')
async def get_prefix():
    return {'prefix': params['prefix']}

@app.get('/test-db')
async def test_db():
    return execute('SELECT * FROM punchcard.event')

@app.get('/get-id')
async def get_name():
    id = ''.join(random.choices(string.ascii_letters + string.digits, k = 5))
    return {'id': id}

@app.post('/post-form')
async def post_form(event_id: str, fields: str):
    return {}

@app.post('/post-event')
async def post_event(host_id: str, title: str, host_name: str, fields: str):
    return {}

@app.get('/get-forms')
async def get_forms(event_id: str):
    return {}

@app.get('/get-event')
async def get_event(host_id: str):
    return {}