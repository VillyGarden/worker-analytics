from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import text
from .db import engine

app = FastAPI(title="Worker Analytics")

@app.get("/")
def root():
    return {"status": "ok", "message": "Worker Analytics API работает"}

@app.get("/health/db")
def health_db():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"db": "ok"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"db": "error", "detail": str(e)})
