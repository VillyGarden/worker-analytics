from fastapi import FastAPI

app = FastAPI(title="Worker Analytics")

@app.get("/")
def root():
    return {"status": "ok", "message": "Worker Analytics API работает"}
