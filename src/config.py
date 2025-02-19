from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str = 'localhost'
    DB_PORT: int = 5432
    DB_NAME: str = 'etl_db'

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


settings = Settings()
