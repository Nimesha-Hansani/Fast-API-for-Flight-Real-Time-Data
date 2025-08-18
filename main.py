import threading
import time
from fastapi import FastAPI
from contextlib import asynccontextmanager
import uvicorn
import requests
from datetime import datetime, timedelta, timezone
import json



latest_flights = []

API_KEY = '182b79ca87eb3234b6ff2664e1a7a6dd'  # Use env variable in App Runner
API_URL = "http://api.aviationstack.com/v1/flights"




#http://api.aviationstack.com/v1/flights?access_key=182b79ca87eb3234b6ff2664e1a7a6dd&updated_after=2025-08-18T17:05:00+00:00


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



def background_fetch():

    global latest_flights
    # offset = load_offset()

    while True:

        flights = fetch_flights()

        try:
         if flights:
                latest_flights = flights  # Replace old data with latest
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

@app.get("/flights")
def get_flights():
    return latest_flights


if __name__ == "__main__":
   
    uvicorn.run(app, host="0.0.0.0", port=8000)
