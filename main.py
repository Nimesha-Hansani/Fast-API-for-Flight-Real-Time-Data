import threading
import time
from fastapi import FastAPI
from contextlib import asynccontextmanager
import uvicorn
import requests
from datetime import datetime, timedelta, timezone
import json
import os
import boto3


latest_flights = []

API_KEY = os.getenv("API_KEY") # Use env variable in App Runner
API_URL = "http://api.aviationstack.com/v1/flights"

# AWS Kinesis settings
KINESIS_STREAM_NAME =  "Flight-API-Data-Stream"
REGION_NAME =  'us-east-1'
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")



kinesis_client = boto3.client("kinesis", 
            region_name=REGION_NAME, 
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
#http://api.aviationstack.com/v1/flights?access_key=182b79ca87eb3234b6ff2664e1a7a6dd&updated_after=2025-08-18T17:05:00+00:00


def fetch_flights():

 
    ten_minutes_ago = datetime.now(timezone.utc) - timedelta(minutes=10)
    updated_after = ten_minutes_ago.strftime("%Y-%m-%dT%H:%M:%S%z")
    print(updated_after)
    params = {
        "access_key": API_KEY,
        "updated_after": updated_after,
        "limit": 2
    }

    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    data = response.json()
    
    data = response.json()
    return data.get("data", [])



def fetch_flights():

 
    ten_minutes_ago = datetime.now(timezone.utc) - timedelta(minutes=10)
    updated_after = ten_minutes_ago.strftime("%Y-%m-%dT%H:%M:%S%z")
    print(updated_after)
    params = {
        "access_key": API_KEY,
        "updated_after": updated_after
    }

    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    data = response.json()
    
    data = response.json()
    return data.get("data", [])


def send_batch_to_kinesis(flights):

    print(flights)
    """Send all flights in one batch to AWS Kinesis"""
    if not flights:
        return
    
    try:
        records = []
        for flight in flights:
            
            partition_key = flight.get("flight_date", "unknown")
            records.append({
                    "Data": json.dumps(flight),
                    "PartitionKey": partition_key
                })

        # Put all records at once
     
        response = kinesis_client.put_records(
                StreamName=KINESIS_STREAM_NAME,
                Records=records
            ) 

        return  
    except Exception as e:
        print(f"Error sending batch to Kinesis: {e}")


def background_fetch():

    # global latest_flights
    # offset = load_offset()

    while True:

        flights = fetch_flights()

        try:
         if flights:
                
                send_batch_to_kinesis(flights)
                # latest_flights = flights  # Replace old data with latest
                print(f"Fetched {len(flights)} flights at {datetime.now(timezone.utc)}")

        except Exception as e:
            print(f"Error fetching flights: {e}")

        time.sleep(300)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Run at startup
    thread = threading.Thread(target=background_fetch, daemon=True)
    thread.start()

    yield  # 👈 App runs while this context is active

    # Run at shutdown (cleanup if needed)
    print("Shutting down...")

app = FastAPI(lifespan=lifespan)

# @app.get("/flights")
# def get_flights():
#     return latest_flights

@app.get("/health")
def health_check():
    return {"status": "running", "stream": KINESIS_STREAM_NAME}


if __name__ == "__main__":
   
    uvicorn.run(app, host="0.0.0.0", port=8000)


