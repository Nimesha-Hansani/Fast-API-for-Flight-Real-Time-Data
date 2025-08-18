import threading
import time
from fastapi import FastAPI
from contextlib import asynccontextmanager
import uvicorn

latest_flights = []

def background_fetch():
    while True:
        latest_flights.append({"flight": "EK123", "status": "On Time"})
        time.sleep(10)

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
