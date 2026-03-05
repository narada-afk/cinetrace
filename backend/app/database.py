# database.py
# Sets up the SQLAlchemy database connection.
# All other files import 'SessionLocal' and 'Base' from here.

import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Read the database URL from environment variable.
# Default falls back to a local value for easier development.
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://sca:sca@postgres:5432/sca"
)

# The engine is the core connection to the database.
engine = create_engine(DATABASE_URL)

# SessionLocal is a factory for creating new DB sessions.
# Each request gets its own session (opened and closed per request).
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base is the parent class for all SQLAlchemy models (Actor, Movie, Cast).
Base = declarative_base()


def get_db():
    """
    FastAPI dependency that provides a database session per request.
    Automatically closes the session when the request is done.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
