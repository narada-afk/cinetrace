# seed_data.py
# Inserts demo actors, movies, and cast entries into the database.
# Run this once after the database tables are created to populate test data.
#
# Usage (inside Docker or locally):
#   python -m app.seed_data

from .database import SessionLocal, engine
from . import models

# Make sure tables exist before inserting data
models.Base.metadata.create_all(bind=engine)


def seed():
    db = SessionLocal()

    # Skip seeding if actors already exist (prevents duplicate entries on re-run)
    if db.query(models.Actor).count() > 0:
        print("Database already has data. Skipping seed.")
        db.close()
        return

    print("Seeding database with demo data...")

    # -----------------------------------------------------------------------
    # Create Actors
    # -----------------------------------------------------------------------
    actors = [
        models.Actor(name="Allu Arjun",   industry="Telugu", debut_year=2003),
        models.Actor(name="Vijay",         industry="Tamil",  debut_year=1992),
        models.Actor(name="Prabhas",       industry="Telugu", debut_year=2002),
        models.Actor(name="Mahesh Babu",   industry="Telugu", debut_year=1999),
    ]
    db.add_all(actors)
    db.flush()  # Flush so actors get their IDs before we reference them below

    # -----------------------------------------------------------------------
    # Create Movies
    # -----------------------------------------------------------------------
    movies = [
        # Allu Arjun films
        models.Movie(title="Pushpa: The Rise",          release_year=2021, imdb_rating=7.6, box_office=365.0,  industry="Telugu"),
        models.Movie(title="Ala Vaikunthapurramuloo",   release_year=2020, imdb_rating=7.3, box_office=230.0,  industry="Telugu"),
        models.Movie(title="Arya",                      release_year=2004, imdb_rating=7.5, box_office=12.0,   industry="Telugu"),
        models.Movie(title="Bunny",                     release_year=2005, imdb_rating=7.0, box_office=20.0,   industry="Telugu"),
        models.Movie(title="Julayi",                    release_year=2012, imdb_rating=7.4, box_office=60.0,   industry="Telugu"),
        models.Movie(title="Race Gurram",               release_year=2014, imdb_rating=7.2, box_office=100.0,  industry="Telugu"),
        models.Movie(title="S/O Satyamurthy",           release_year=2015, imdb_rating=7.9, box_office=100.0,  industry="Telugu"),
        models.Movie(title="Dj: Duvvada Jagannadham",   release_year=2017, imdb_rating=6.5, box_office=90.0,   industry="Telugu"),
        models.Movie(title="Na Peru Surya",             release_year=2018, imdb_rating=5.5, box_office=40.0,   industry="Telugu"),
        models.Movie(title="Ala Vaikunthapurramuloo",   release_year=2020, imdb_rating=7.3, box_office=230.0,  industry="Telugu"),

        # Vijay films
        models.Movie(title="Ghilli",                    release_year=2004, imdb_rating=7.4, box_office=35.0,   industry="Tamil"),
        models.Movie(title="Sivakasi",                  release_year=2005, imdb_rating=6.8, box_office=30.0,   industry="Tamil"),
        models.Movie(title="Pokkiri",                   release_year=2007, imdb_rating=7.2, box_office=50.0,   industry="Tamil"),
        models.Movie(title="Thuppakki",                 release_year=2012, imdb_rating=8.0, box_office=110.0,  industry="Tamil"),
        models.Movie(title="Kaththi",                   release_year=2014, imdb_rating=7.7, box_office=130.0,  industry="Tamil"),
        models.Movie(title="Mersal",                    release_year=2017, imdb_rating=7.8, box_office=200.0,  industry="Tamil"),
        models.Movie(title="Sarkar",                    release_year=2018, imdb_rating=6.9, box_office=185.0,  industry="Tamil"),
        models.Movie(title="Bigil",                     release_year=2019, imdb_rating=7.5, box_office=300.0,  industry="Tamil"),
        models.Movie(title="Master",                    release_year=2021, imdb_rating=7.7, box_office=250.0,  industry="Tamil"),
        models.Movie(title="Beast",                     release_year=2022, imdb_rating=5.9, box_office=130.0,  industry="Tamil"),

        # Prabhas films
        models.Movie(title="Baahubali: The Beginning",  release_year=2015, imdb_rating=8.0, box_office=600.0,  industry="Telugu"),
        models.Movie(title="Baahubali: The Conclusion", release_year=2017, imdb_rating=8.2, box_office=1800.0, industry="Telugu"),
        models.Movie(title="Saaho",                     release_year=2019, imdb_rating=5.3, box_office=400.0,  industry="Telugu"),
        models.Movie(title="Radhe Shyam",               release_year=2022, imdb_rating=5.4, box_office=200.0,  industry="Telugu"),
        models.Movie(title="Adipurush",                 release_year=2023, imdb_rating=3.5, box_office=370.0,  industry="Telugu"),

        # Mahesh Babu films
        models.Movie(title="Okkadu",                    release_year=2003, imdb_rating=7.8, box_office=25.0,   industry="Telugu"),
        models.Movie(title="Pokiri",                    release_year=2006, imdb_rating=7.9, box_office=80.0,   industry="Telugu"),
        models.Movie(title="Dookudu",                   release_year=2011, imdb_rating=7.4, box_office=100.0,  industry="Telugu"),
        models.Movie(title="Businessman",               release_year=2012, imdb_rating=7.6, box_office=80.0,   industry="Telugu"),
        models.Movie(title="SSMB 28 (Upcoming)",        release_year=2025, imdb_rating=None, box_office=None,  industry="Telugu"),
    ]
    db.add_all(movies)
    db.flush()  # Flush so movies get their IDs

    # -----------------------------------------------------------------------
    # Build a lookup: movie title -> movie object (for easy Cast creation)
    # -----------------------------------------------------------------------
    movie_lookup = {m.title: m for m in movies}
    actor_lookup = {a.name: a for a in actors}

    # -----------------------------------------------------------------------
    # Create Cast entries (link actors to their movies)
    # -----------------------------------------------------------------------
    cast_entries = [
        # Allu Arjun
        models.Cast(actor=actor_lookup["Allu Arjun"], movie=movie_lookup["Pushpa: The Rise"],        role_type="Lead"),
        models.Cast(actor=actor_lookup["Allu Arjun"], movie=movie_lookup["Ala Vaikunthapurramuloo"], role_type="Lead"),
        models.Cast(actor=actor_lookup["Allu Arjun"], movie=movie_lookup["Arya"],                    role_type="Lead"),
        models.Cast(actor=actor_lookup["Allu Arjun"], movie=movie_lookup["Bunny"],                   role_type="Lead"),
        models.Cast(actor=actor_lookup["Allu Arjun"], movie=movie_lookup["Julayi"],                  role_type="Lead"),
        models.Cast(actor=actor_lookup["Allu Arjun"], movie=movie_lookup["Race Gurram"],             role_type="Lead"),
        models.Cast(actor=actor_lookup["Allu Arjun"], movie=movie_lookup["S/O Satyamurthy"],         role_type="Lead"),
        models.Cast(actor=actor_lookup["Allu Arjun"], movie=movie_lookup["Dj: Duvvada Jagannadham"], role_type="Lead"),
        models.Cast(actor=actor_lookup["Allu Arjun"], movie=movie_lookup["Na Peru Surya"],           role_type="Lead"),

        # Vijay
        models.Cast(actor=actor_lookup["Vijay"], movie=movie_lookup["Ghilli"],     role_type="Lead"),
        models.Cast(actor=actor_lookup["Vijay"], movie=movie_lookup["Sivakasi"],   role_type="Lead"),
        models.Cast(actor=actor_lookup["Vijay"], movie=movie_lookup["Pokkiri"],    role_type="Lead"),
        models.Cast(actor=actor_lookup["Vijay"], movie=movie_lookup["Thuppakki"],  role_type="Lead"),
        models.Cast(actor=actor_lookup["Vijay"], movie=movie_lookup["Kaththi"],    role_type="Lead"),
        models.Cast(actor=actor_lookup["Vijay"], movie=movie_lookup["Mersal"],     role_type="Lead"),
        models.Cast(actor=actor_lookup["Vijay"], movie=movie_lookup["Sarkar"],     role_type="Lead"),
        models.Cast(actor=actor_lookup["Vijay"], movie=movie_lookup["Bigil"],      role_type="Lead"),
        models.Cast(actor=actor_lookup["Vijay"], movie=movie_lookup["Master"],     role_type="Lead"),
        models.Cast(actor=actor_lookup["Vijay"], movie=movie_lookup["Beast"],      role_type="Lead"),

        # Prabhas
        models.Cast(actor=actor_lookup["Prabhas"], movie=movie_lookup["Baahubali: The Beginning"],  role_type="Lead"),
        models.Cast(actor=actor_lookup["Prabhas"], movie=movie_lookup["Baahubali: The Conclusion"], role_type="Lead"),
        models.Cast(actor=actor_lookup["Prabhas"], movie=movie_lookup["Saaho"],                     role_type="Lead"),
        models.Cast(actor=actor_lookup["Prabhas"], movie=movie_lookup["Radhe Shyam"],               role_type="Lead"),
        models.Cast(actor=actor_lookup["Prabhas"], movie=movie_lookup["Adipurush"],                 role_type="Lead"),

        # Mahesh Babu
        models.Cast(actor=actor_lookup["Mahesh Babu"], movie=movie_lookup["Okkadu"],             role_type="Lead"),
        models.Cast(actor=actor_lookup["Mahesh Babu"], movie=movie_lookup["Pokiri"],             role_type="Lead"),
        models.Cast(actor=actor_lookup["Mahesh Babu"], movie=movie_lookup["Dookudu"],            role_type="Lead"),
        models.Cast(actor=actor_lookup["Mahesh Babu"], movie=movie_lookup["Businessman"],        role_type="Lead"),
        models.Cast(actor=actor_lookup["Mahesh Babu"], movie=movie_lookup["SSMB 28 (Upcoming)"], role_type="Lead"),
    ]
    db.add_all(cast_entries)

    db.commit()
    print("Seed complete! Inserted actors, movies, and cast entries.")
    db.close()


if __name__ == "__main__":
    seed()
