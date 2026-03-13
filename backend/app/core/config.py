from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application configuration loaded from environment variables and/or a
    .env file in the project root.

    All fields have sensible defaults for local development. Override any
    field by setting the corresponding environment variable (case-insensitive).

    Example .env:
        DATABASE_URL=sqlite:///./bizagent.db
        UPLOAD_DIR=./uploads
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    APP_NAME: str = "BizAgent"
    APP_VERSION: str = "0.1.0"

    # SQLite file at project root for MVP. Swap to postgresql+psycopg2://...
    # for production without touching any other file.
    DATABASE_URL: str = "sqlite:///./bizagent.db"

    # Directory where uploaded CSV files are persisted before processing.
    UPLOAD_DIR: str = "./uploads"

    # --- AI Insights (Step 3) ---
    # Set OPENAI_API_KEY to enable real LLM calls; leave empty for stub mode.
    OPENAI_API_KEY: str = ""
    # OpenAI model used for insight generation. gpt-4o-mini balances quality
    # and cost well for structured JSON extraction tasks.
    INSIGHTS_MODEL: str = "gpt-4o-mini"


# Module-level singleton — import `settings` everywhere; never instantiate
# Settings() directly in other modules.
settings = Settings()
