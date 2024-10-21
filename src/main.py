import os
import uvicorn
import pandas as pd
import logging
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

@app.get("/slice")
async def location_slice(latitude_range: str, longitude_range: str):
    latitude_range = latitude_range.split(",")
    longitude_range = longitude_range.split(",")
    try:
        lat_start = float(latitude_range[0])
        lat_end = float(latitude_range[1])
        long_start = float(longitude_range[0])
        long_end = float(longitude_range[1])
    except:
        raise HTTPException(status_code=400, detail="Latitude and Longitude must be numbers")
    
    generator = ais_lat_long_slice_generator((lat_start, lat_end), (long_start, long_end))

    return StreamingResponse(generator, media_type="text/event_stream")

async def ais_lat_long_slice_generator(latitude_range: tuple, longitude_range: tuple):
    while True:  
        data: pd.DataFrame = await get_current_ais_data()
        data = data[data["Latitude"].between(latitude_range[0], latitude_range[1]) & data["Longitude"].between(longitude_range[0], longitude_range[1])]
        yield data.to_json(orient='records')
        await sleep(10)
        
async def ais_data_generator():
    global current_hour_ais_data
    global ais_data
    if current_hour_ais_data != datetime.now().hour or ais_data is None:
        current_hour_ais_data = datetime.now().hour
        ais_data = pd.read_feather(DATA_FILE_PATH+'aisdk-2024-09-09-hour-' + str(datetime.now().hour) + '.feather')
        ais_data['Time'] = ais_data['# Timestamp'].dt.time.astype(str)
    while True: 
        ais_data_current_time = get_current_ais_data()
        data = ais_data_current_time.to_json(orient='records')
        yield 'event: ais\n' + 'data: ' + data + '\n\n'
        await sleep(1)

async def get_current_ais_data():
    global ais_data
    current_timestamp = datetime.now().time().strftime("%H:%M:%S")
    return ais_data[ais_data['Time'] == current_timestamp]


if __name__ == "__main__":
    uvicorn.run("main:app", host=SOURCE_IP, port=SOURCE_PORT, reload=True)
