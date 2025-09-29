from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "worker_analytics"
    DB_USER: str = "worker"
    DB_PASS: str = ""

    MS_API_TOKEN: str = ""
    MS_BASE_URL: str = "https://online.moysklad.ru/api/remap/1.2"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

settings = Settings()
