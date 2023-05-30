import random
import string
import json
import time
import os
import urllib.parse

from fastapi import FastAPI
from mangum import Mangum
import boto3
from pydantic import BaseModel
from fastapi_utils.tasks import repeat_every

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

def execute(sql, type = 'GET', args = []):
    response = rds_client.execute_statement(
        secretArn = os.environ.get('database_credentials_secret_store_arn'),
        database = os.environ.get('database_name'),
        resourceArn = os.environ.get('database_cluster_arn'),
        sql = sql,
        parameters = args
    )
    
    if type in ['POST', 'UPDATE', 'DELETE']:
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return {'status': 'success'}  
        else:
            return {'status': 'error', 'message': 'Could modify into database'}

    elif type == 'GET':
        return response['records']

    return {'status': 'error', 'message': 'invalid type'}

@app.get('/')
async def root():
    return {'message': 'its all good'}

@app.get('/get-id')
async def get_id():
    id = ''.join(random.choices(string.ascii_letters + string.digits, k = params['id_length']))
    return {'id': id}

@app.post('/post-form')
async def post_form(form: Form):
    try: 
        event_title = urllib.parse.unquote_plus(form.event_title)
        fields = urllib.parse.unquote_plus(form.fields)

        response = execute(f'SELECT * FROM event WHERE host_id = "{form.host_id}" AND title = "{event_title}"')

        if len(response) == 0:
            return {'status': 'error', 'message': 'event does not exist'}
        
        if execute(f'SELECT * FROM form WHERE host_id = "{form.host_id}" AND event_title = "{form.event_title}" AND id = "{form.id}"'):
            return {'status': 'error', 'message': 'Form already submitted for this event'}

        event = response[0]
        event_fields = json.loads(event[params['event_keys'].index('fields')]['stringValue'])

        form_fields = json.loads(fields)

        if set([field['name'] for field in event_fields]) != set([field['name'] for field in form_fields]):
            return {'status': 'error', 'message': 'form fields do not match event fields'}

        for event_field, form_field in zip(event_fields, form_fields):
            name, value = form_field['name'], form_field['value']
            field_type, field_presence = event_field['type'], event_field['presence']

            if field_presence == 'required' and not value:
                return {'status': 'error', 'message': name + ' field is required'}

            if (field_type == 'integer' and (value and not value.isnumeric())):
                return {'status': 'error', 'message': name + ' field is the incorrect type'}

        args = [
            {'name': 'id', 'value': {'stringValue': form.id}},
            {'name': 'host_id', 'value': {'stringValue': form.host_id}},
            {'name': 'event_title', 'value': {'stringValue': event_title}},
            {'name': 'fields', 'value': {'stringValue': fields}}
        ]

        return execute('INSERT INTO form VALUES(:id, :host_id, :event_title, :fields)', 'POST', args)

    except:
        return {'status': 'error', 'message': 'Database is unresponsive'}
    
@app.post('/post-event')
async def post_event(event: Event):
    try:
        title = urllib.parse.unquote_plus(event.title)
        host_name = urllib.parse.unquote_plus(event.host_name)
        fields = json.loads(urllib.parse.unquote_plus(event.fields))
        
        if execute(f'SELECT * FROM event WHERE host_id = "{event.host_id}" AND title = "{title}"'):
            return {'status': 'error', 'message': 'Cannot create event with duplicate title'}

        required_flag = True
        
        for field in fields:
            if set([key for key in field]) != set(['name', 'type', 'presence']):
                return {'status': 'error', 'message': 'field not formatted correctly'}
     
            name, field_type, field_presence = field['name'], field['type'], field['presence']

            if field_type not in params['field_types']:
                return {'status': 'error', 'message': name + ' field data type not supported'}

            if field_presence not in params['field_presences']:
                return {'status': 'error', 'message': name + ' field data presence not supported'}

            if required_flag and field_presence == 'required':
                required_flag = False

        if required_flag:
            return {'status': 'error', 'message': 'Must have at least one required field'}
        
        fields = json.dumps(fields)

        args = [
            {'name': 'host_id', 'value': {'stringValue': event.host_id}},
            {'name': 'title', 'value': {'stringValue': title}},
            {'name': 'host_name', 'value': {'stringValue': host_name}},
            {'name': 'fields', 'value': {'stringValue': fields}},
            {'name': 'timestamp', 'value': {'longValue': int(time.time())}}
        ]

        return execute(f'INSERT INTO event VALUES(:host_id, :title, :host_name, :fields, :timestamp)', 'POST', args)

    except:
        return {'status': 'error', 'message': 'Database is unresponsive'}

@app.get('/get-forms')
async def get_forms(host_id: str, event_title: str):
    try:
        event_title = urllib.parse.unquote_plus(event_title)

        response = execute(f'SELECT * FROM form WHERE host_id = "{host_id}" AND event_title = "{event_title}"')
        forms = []

        for values in response:
            form = {}

            for key, value in zip(params['form_keys'], values):
                if key == 'fields':
                    form[key] = json.loads(value['stringValue'])
                else:
                    form[key] = value['stringValue']

            forms.append(form)
        
        return {'forms': forms}

    except:
        return {'status': 'error', 'message': 'Database is unresponsive'}

@app.get('/get-events')
async def get_events(host_id: str):
    try: 
        response = execute(f'SELECT * FROM event WHERE host_id = "{host_id}"')
        events = []

        for values in response:
            event = {}
            expired = False

            for key, value in zip(params['event_keys'], values):
                if key == 'timestamp':
                    event[key] = value['longValue']

                    if int(time.time()) >= value['longValue'] + params['event_lifetime']:
                        expired = True

                elif key == 'fields':
                    event[key] = json.loads(value['stringValue'])
                else:
                    event[key] = value['stringValue']
                
            if not expired:
                events.append(event)

        return {'events': events}

    except:
        return {'status': 'error', 'message': 'Database is unresponsive'}

@app.delete('/delete-event')
async def delete_event(host_id: str, event_title: str):
    try:
        event_title = urllib.parse.unquote_plus(event_title)

        response = execute(f'SELECT * FROM event WHERE host_id = "{host_id}" AND title = "{event_title}"')
        
        if len(response) == 0:
            return {'status': 'error', 'message': 'event does not exist'}

        form_response = execute(f'DELETE FROM form WHERE host_id = "{host_id}" AND event_title = "{event_title}"', 'DELETE')
        event_response = execute(f'DELETE FROM event WHERE host_id = "{host_id}" AND title = "{event_title}"', 'DELETE')

        if event_response['status'] == form_response['status'] == 'success':
            return event_response

        return {'status': 'error', 'message': 'could not delete event and forms'}

    except:
        return {'status': 'error', 'message': 'Database is unresponsive'}

@app.on_event('startup')
@repeat_every(seconds = 300)
async def delete_expired_events():
    expired_timestamp = int(time.time()) - params['event_lifetime']
    response = execute(f'SELECT * FROM event WHERE timestamp <= {expired_timestamp}')

    for event in response:
        host_id = event[params['event_keys'].index('host_id')]['stringValue']
        title = event[params['event_keys'].index('title')]['stringValue']

        execute(f'DELETE FROM form WHERE host_id = "{host_id}" AND event_title = "{title}"', 'DELETE')
        execute(f'DELETE FROM event WHERE host_id = "{host_id}" AND title = "{title}"', 'DELETE')
