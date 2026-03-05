# models.py
# Defines the database tables as Python classes using SQLAlchemy.
# Each class = one table in PostgreSQL.

from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship
from .database import Base


class Actor(Base):
    """
    Represents a South Indian film actor.
    One actor can appear in many movies (through the Cast table).
    """
    __tablename__ = "actors"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False)       # e.g. "Allu Arjun"
    industry = Column(String, nullable=False)                # e.g. "Telugu", "Tamil"
    debut_year = Column(Integer, nullable=True)              # e.g. 2003

    # Relationship: Actor -> Cast -> Movie
    cast_entries = relationship("Cast", back_populates="actor")


class Movie(Base):
    """
    Represents a South Indian film.
    One movie can have many actors (through the Cast table).
    """
    __tablename__ = "movies"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)                   # e.g. "Pushpa"
    release_year = Column(Integer, nullable=False)           # e.g. 2021
    imdb_rating = Column(Float, nullable=True)               # e.g. 7.6
    box_office = Column(Float, nullable=True)                # in crores (INR)
    industry = Column(String, nullable=False)                # e.g. "Telugu"

    # Relationship: Movie -> Cast -> Actor
    cast_entries = relationship("Cast", back_populates="movie")


class Cast(Base):
    """
    Join table linking actors to movies.
    Also stores what role the actor played (e.g. "Lead", "Supporting").
    """
    __tablename__ = "cast"

    id = Column(Integer, primary_key=True, index=True)
    actor_id = Column(Integer, ForeignKey("actors.id"), nullable=False)
    movie_id = Column(Integer, ForeignKey("movies.id"), nullable=False)
    role_type = Column(String, nullable=True)                # e.g. "Lead", "Supporting"

    # Back-references so we can navigate: cast.actor or cast.movie
    actor = relationship("Actor", back_populates="cast_entries")
    movie = relationship("Movie", back_populates="cast_entries")
