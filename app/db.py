from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from .config import settings

class Base(DeclarativeBase):
    pass

def _db_url() -> str:
    return (
        f"postgresql+psycopg://{settings.DB_USER}:{settings.DB_PASS}"
        f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    )

engine = create_engine(_db_url(), pool_pre_ping=True, pool_size=5, max_overflow=10)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

# FastAPI dependency
def get_session():
    with SessionLocal() as session:
        yield session
