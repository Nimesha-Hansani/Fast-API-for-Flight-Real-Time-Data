import threading
import time
from fastapi import FastAPI
from contextlib import asynccontextmanager
import uvicorn
import requests


API_KEY = 'b9a99cfbc2971adba7d9b72b3264a66d'  # Use env variable in App Runner
API_URL = "http://api.aviationstack.com/v1/flights"
# OFFSET_FILE = "offset.json"
LIMIT = 5
# FETCH_INTERVAL = 10 

latest_flights = []

def fetch_flights():

    params = {
        "access_key": API_KEY,
        "limit": LIMIT
    }
     
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    data = response.json()
    
    data = response.json()
    return data.get("data", [])


def background_fetch():

    flights = fetch_flights()
    latest_flights.extend(flights)
    


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
