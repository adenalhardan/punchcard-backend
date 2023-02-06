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

class Form(BaseModel):
    id: str
    host_id: str
    event_title: str
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
async def post_form(form: Form):
    event_title = urllib.parse.unquote_plus(form.event_title)
    fields = urllib.parse.unquote_plus(form.fields)

    response = execute(f'SELECT * FROM punchcard.event WHERE host_id = "{form.host_id}" AND title = "{event_title}"')

    if len(response) == 0:
        return {'status': 'error', 'message': 'event does not exist'}

    event = response[0]
    event_fields = json.loads(event[3]['stringValue'])

    form_fields = json.loads(fields)

    if [field['name'] for field in event_fields] != [field['name'] for field in form_fields]:
        return {'status': 'error', 'message': 'form fields do not match event fields'}

    for event_field, form_field in zip(event_fields, form_fields):
        name, data = form_field['name'], form_field['value']
        field_type, field_presence = event_field['type'], event_field['presence']

        if field_presence == 'required' and not data:
            return {'status': 'error', 'message': name + 'field is required'}

        if (field_type == 'integer' and type(data) is not int) or (field_type == 'string' and type(data) is not str):
            return {'status': 'error', 'message': name + 'field is the incorrect type'}

    args = [
        {'name': 'id', 'value': {'stringValue': form.id}},
        {'name': 'host_id', 'value': {'stringValue': form.host_id}},
        {'name': 'event_title', 'value': {'stringValue': event_title}},
        {'name': 'fields', 'value': {'stringValue': fields}}
    ]

    return execute('INSERT INTO punchcard.form VALUES(:id, :host_id, :event_title, :fields)', args)
    

@app.post('/post-event')
async def post_event(event: Event):
    title = urllib.parse.unquote_plus(event.title)
    host_name = urllib.parse.unquote_plus(event.host_name)
    fields = json.loads(urllib.parse.unquote_plus(event.fields))
    
    if len(execute(f'SELECT * FROM punchcard.event WHERE host_id = "{event.host_id}" AND title = "{title}"')) > 0:
        return {'status': 'error', 'message': 'host already created event of same title'}
    
    for field in fields:
        if set([key for key in field]) != set(['name', 'type', 'presence']):
            return {'status': 'error', 'message': 'field not formatted correctly'}
 
        name, field_type, field_presence = field['name'], field['type'], field['presence']

        if field_type not in params['field_types']:
            return {'status': 'error', 'message': name + ' field data type not supported'}

        if field_presence not in params['field_presences']:
            return {'status': 'error', 'message': name + ' field data presence not supported'}
    
    fields = json.dumps(fields)

    args = [
        {'name': 'host_id', 'value': {'stringValue': event.host_id}},
        {'name': 'title', 'value': {'stringValue': title}},
        {'name': 'host_name', 'value': {'stringValue': host_name}},
        {'name': 'fields', 'value': {'stringValue': fields}}
    ]

    return execute(f'INSERT INTO punchcard.event VALUES(:host_id, :title, :host_name, :fields)', args)

@app.get('/get-forms')
async def get_forms(host_id: str, event_title: str):
    event_title = urllib.parse.unquote_plus(event_title)

    response = execute(f'SELECT * FROM punchcard.form WHERE host_id = "{host_id}" AND event_title = "{event_title}"')
    forms = []

    for values in response:
        form = {}

        for key, value in zip(params['form_keys'], values):
            if key == 'fields':
                form[key] = json.loads(value['stringValue'])
            else:
                form[key] = value['stringValue']

        forms.append(form)
    
    return forms

@app.get('/get-events')
async def get_events(host_id: str):
    response = execute(f'SELECT * FROM punchcard.event WHERE host_id = "{host_id}"')
    events = []

    for values in response:
        event = {}

        for key, value in zip(params['event_keys'], values):
            if key == 'fields':
                event[key] = json.loads(value['stringValue'])
            else:
                event[key] = value['stringValue']

        events.append(event)

    return events