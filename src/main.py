import os
import json
import uvicorn
import pandas as pd
import logging
import datetime
from fastapi import FastAPI, Response, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from asyncio import sleep
from dotenv import load_dotenv, find_dotenv
from datetime import datetime
from typing import Any, Awaitable, TypeVar

load_dotenv('.env')
DATA_SUPPLIER_SERVER = os.getenv('PATH_TO_DATA_FOLDER')
DATA_FILE_PATH = str(os.getenv('PATH_TO_DATA_FOLDER'))
WORKERS = int(os.getenv('WORKERS'))
SOURCE_IP = os.getenv('SOURCE_IP')
SOURCE_PORT = int(os.getenv('SOURCE_PORT'))

logger = logging.getLogger('uvicorn.error')

current_hour_ais_data = datetime.now().hour
ais_data = pd.read_feather(DATA_FILE_PATH+'aisdk-2024-09-09-hour-' + str(datetime.now().hour) + '.feather')

# Precompute the time strings for comparison
ais_data['Time'] = ais_data['# Timestamp'].dt.time.astype(str)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

T = TypeVar("T")

@app.get("/dummy-ais-data")
async def ais_data_fetch(request: Request):
    generator = ais_data_generator()
    return StreamingResponse(generator, media_type="text/event-stream")

async def ais_data_generator():
    global current_hour_ais_data
    global ais_data
    if current_hour_ais_data != datetime.now().hour or ais_data is None:
        current_hour_ais_data = datetime.now().hour
        ais_data = pd.read_feather(DATA_FILE_PATH+'aisdk-2024-09-09-hour-' + str(datetime.now().hour) + '.feather')
        ais_data['Time'] = ais_data['# Timestamp'].dt.time.astype(str)
    while True: 
        current_timestamp = datetime.now().time().strftime("%H:%M:%S")
        ais_data_current_time = ais_data[ais_data['Time'] == current_timestamp]
        data = ais_data_current_time.to_json(orient='records')
        yield data + '\n\n'
        await sleep(1)

if __name__ == "__main__":
    uvicorn.run("main:app", host=SOURCE_IP, port=SOURCE_PORT, reload=True)
