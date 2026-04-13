from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173"
    jwt_secret: str = "dev-secret-change-me"
    jwt_algorithm: str = "HS256"
    jwt_exp_hours: int = 72
    # כתובת ציבורית של ה-API לתוסף WordPress (בפרודקשן: https://api.example.com)
    public_api_base: str = "http://127.0.0.1:8000"
    ai_chat_enabled: bool = true
    openai_api_key: str | None = None
    ai_chat_model: str = "gpt-4o-mini"


settings = Settings()
