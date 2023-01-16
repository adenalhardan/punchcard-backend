from fastapi import FastAPI
from mangum import Mangum
import boto3

import random
import string

app = FastAPI()
handler = Mangum(app)

database_name = 'punchcard'
database_cluster_arn = 'arn:aws:rds:us-west-1:648352157129:cluster:punchcard'
database_credentials_secret_store_arn = 'arn:aws:secretsmanager:us-west-1:648352157129:secret:rds-db-credentials/cluster-E2JST6UE4XRFDW2SVOKMPWGEIQ/admin/1673905435773-76ZLCx'

rds_client = boto3.client('rds-data')

prefix = 'punchcard:'

@app.get('/')
async def root():
    return {'message': 'its all good'}

@app.get('/get-name')
async def get_name():
    name = ''.join(random.choices(string.ascii_letters + string.digits, k = 5))
    return {'name': prefix + name}

@app.get('/test-db')
async def test_db():
    return execute('SELECT * FROM punchcard.event')
    
def execute(sql):
    response = rds_client.execute_statement(
        secretArn = database_credentials_secret_store_arn,
        database = database_name,
        resourceArn = database_cluster_arn,
        sql = sql
    )
    return response['records']