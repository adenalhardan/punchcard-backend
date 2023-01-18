import random
import string
import json
import urllib.parse

from fastapi import FastAPI
from mangum import Mangum
import boto3
from pydantic import BaseModel

params = json.load(open('params.json'))

class Event(BaseModel):
    host_id: str
    title: str
    host_name: str
    fields: str

app = FastAPI()
handler = Mangum(app)

rds_client = boto3.client('rds-data', region_name = 'us-west-1')

def execute(sql, args = []):
    response = rds_client.execute_statement(
        secretArn = params['database_credentials_secret_store_arn'],
        database = params['database_name'],
        resourceArn = params['database_cluster_arn'],
        sql = sql,
        parameters = args
    )

    if args:
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return {'status': 'success'}  
        else:
            return {'status': 'error', 'message': 'could not insert into database'}

    return response['records']

@app.get('/')
async def root():
    return {'message': 'its all good'}

@app.get('/get-prefix')
async def get_prefix():
    return {'prefix': params['prefix']}

@app.get('/get-id')
async def get_name():
    id = ''.join(random.choices(string.ascii_letters + string.digits, k = params['id_length']))
    return {'id': id}

@app.post('/post-form')
async def post_form(id: str, host_id: str, event_title: str, fields: str):
    event_title = urllib.parse.unquote_plus(event_title)
    fields = urllib.unquote_plus(fields)

    if not execute(f'SELECT * FROM punchcard.event WHERE host_id = {host_id} AND title = {event_title}'):
        return {'status': 'error', 'message': 'event does not exist'}

    event = execute(f'SELECT * FROM punchcard.event WHERE host_id = {host_id} AND title = {event_title}')[0]
    event_fields = json.loads(event['fields'])

    form_fields = json.loads(fields)

    if set([name for name in event_fields]) != set([name for name in form_fields]):
        return {'status': 'error', 'message': 'form fields do not match event fields'}

    for name in form_fields:
        data = form_fields[name]
        data_type, data_presence = event_fields[name]['data_type'], event_fields[name]['data_presence']

        if data_presence == 'required' and not data:
            return {'status': 'error', 'message': name + 'field is required'}

        if (data_type == 'integer' and type(data) is not int) or (data_type == 'string' and type(data) is not str):
            return {'status': 'error', 'message': name + 'field is the incorrect type'}

    args = [
        {'name': 'id', 'value': {'stringValue': id}},
        {'name': 'host_id', 'value': {'stringValue': host_id}},
        {'name': 'event_title', 'value': {'stringValue': event_title}},
        {'name': 'fields', 'value': {'stringValue': fields}}
    ]

    return execute('INSERT INTO punchcard.form VALUES(:id, :host_id, :event_title, :fields)', args)
    

@app.post('/post-event')
async def post_event(host_id: str, title: str, host_name: str, fields: str):
    title = urllib.parse.unquote_plus(title)
    host_name = urllib.parse.unquote_plus(host_name)
    fields = json.loads(urllib.parse.unquote_plus(fields))

    if len(execute(f'SELECT * FROM punchcard.event WHERE host_id = {host_id} AND title = {title}')) > 0:
        return {'status': 'error', 'message': 'host already created event of same title'}
    
    for name in fields:
        if set([key for key in fields[name]]) != set(['data_type', 'data_presence']):
            return {'status': 'error', 'message': name + ' field not formatted correctly'}
 
        data_type, data_presence = fields[name]['data_type'], fields[name]['data_presence']

        if data_type not in params['data_types']:
            return {'status': 'error', 'message': name + ' field data type not supported'}

        if data_presence not in params['data_presences']:
            return {'status': 'error', 'message': name + ' field data presence not supported'}

    fields = json.dumps(fields)

    args = [
        {'name': 'host_id', 'value': {'stringValue': host_id}},
        {'name': 'title', 'value': {'stringValue': title}},
        {'name': 'host_name', 'value': {'stringValue': host_name}},
        {'name': 'fields', 'value': {'stringValue': fields}}
    ]

    return execute(f'INSERT INTO punchcard.event VALUES(:host_id, :title, :host_name, :fields)', args)

@app.get('/get-forms')
async def get_forms(host_id: str, event_title: str):
    event_title = urllib.parse.unquote_plus(event_title)

    return execute(f'SELECT * FROM punchcard.form WHERE host_id = {host_id} AND event_title = {event_title}')

@app.get('/get-events')
async def get_events(host_id: str):
    return execute(f'SELECT * FROM punchcard.event WHERE host_id = {host_id}')

@app.post('/test-db')
async def test_db(event: Event):
    fields = urllib.parse.unquote_plus(event.fields)

    args = [
        {'name': 'host_id', 'value': {'stringValue': event.host_id}},
        {'name': 'title', 'value': {'stringValue': urllib.parse.unquote_plus(event.title)}},
        {'name': 'host_name', 'value': {'stringValue': urllib.parse.unquote_plus(event.host_name)}},
        {'name': 'fields', 'value': {'stringValue': fields}}
    ]
    return {
        "res": execute(f'INSERT INTO punchcard.event VALUES(:host_id, :title, :host_name, :fields)', args),
        "fields": json.loads(fields)
    }
