"""
alembic/env.py
==============
Alembic environment script — runs for every alembic command.

Reads DATABASE_URL from the environment (same variable the app uses).
Imports the SQLAlchemy Base so --autogenerate can diff the live schema
against the models defined in app/models.py.
"""

import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context

# ── Pull in app models so autogenerate can see the full schema ────────────────
# Import Base first so models register themselves against it.
from app.database import Base          # noqa: F401 (needed for side-effects)
import app.models                      # noqa: F401 (registers all table classes)

# ── Alembic Config object ─────────────────────────────────────────────────────
config = context.config

# Override sqlalchemy.url from environment — same variable the app uses.
database_url = os.environ.get("DATABASE_URL")
if database_url:
    config.set_main_option("sqlalchemy.url", database_url)

# Set up Python logging from alembic.ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# The metadata object that --autogenerate compares against
target_metadata = Base.metadata


# ── Offline mode (generate SQL without connecting) ───────────────────────────

def run_migrations_offline() -> None:
    """Emit SQL to stdout without a live DB connection."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


# ── Online mode (connect and run) ─────────────────────────────────────────────

def run_migrations_online() -> None:
    """Run migrations against a live database connection."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,   # no persistent pool — one connection per run
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
