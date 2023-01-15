from fastapi import FastAPI
from mangum import Mangum

import random
import string

app = FastAPI()
handler = Mangum(app)

prefix = 'punchcard:'

@app.get('/')
async def root():
    return {'message': 'its all good'}

@app.get('/get-name')
async def get_name():
    name = ''.join(random.choices(string.ascii_letters + string.digits, k = 5))
    return {'name': prefix + name}