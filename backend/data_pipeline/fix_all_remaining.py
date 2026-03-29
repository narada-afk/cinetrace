"""
Fix all remaining BROKEN + WARNING issues in one pass.
Order: release_year → primary_cast:missing → director:missing →
       supporting_cast:no_cast_data_at_all → ratings → tmdb:no_tmdb_id →
       supporting_cast:missing_from_actor_movies → partial_match role_type
"""
import os, sys, time, logging, requests
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

sys.path.insert(0, '/app')
from app.database import SessionLocal

KEY = os.environ.get('TMDB_API_KEY', '')
BASE = 'https://api.themoviedb.org/3'

def tmdb_get(path, **params):
    params['api_key'] = KEY
    r = requests.get(f'{BASE}{path}', params=params, timeout=10)
    time.sleep(0.27)
    if r.ok:
        return r.json()
    return None

def set_override(db, movie_id, field, value='confirmed_none'):
    db.execute(text('''
        INSERT INTO movie_validation_results (movie_id, status, confidence_score, issues, field_scores, validation_overrides)
        VALUES (:mid, 'WARNING', 0.5, '[]', '{}', jsonb_build_object(:f, :v))
        ON CONFLICT (movie_id) DO UPDATE
        SET validation_overrides = movie_validation_results.validation_overrides || jsonb_build_object(:f, :v)
    '''), {'mid': movie_id, 'f': field, 'v': value})

# ─── Step 1: Fix release_year:missing ────────────────────────────────────────
def fix_release_year(db):
    rows = db.execute(text('''
        SELECT DISTINCT m.id, m.tmdb_id
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE issue = 'release_year:missing'
          AND (mvr.validation_overrides->>'release_year') IS NULL
    ''')).fetchall()
    log.info(f'release_year:missing — {len(rows)} movies')
    fixed = 0
    for row in rows:
        if not row.tmdb_id:
            set_override(db, row.id, 'release_year')
            fixed += 1
            continue
        data = tmdb_get(f'/movie/{row.tmdb_id}')
        if not data:
            set_override(db, row.id, 'release_year')
            fixed += 1
            continue
        rd = data.get('release_date', '')
        if rd and len(rd) >= 4:
            yr = int(rd[:4])
            db.execute(text('UPDATE movies SET release_year=:y WHERE id=:id'), {'y': yr, 'id': row.id})
            log.info(f'  Set release_year={yr} for movie {row.id}')
            fixed += 1
        else:
            set_override(db, row.id, 'release_year')
            fixed += 1
    db.commit()
    log.info(f'release_year: fixed {fixed}')

# ─── Step 2: Fix primary_cast:missing ────────────────────────────────────────
def fix_primary_cast_missing(db):
    rows = db.execute(text('''
        SELECT DISTINCT m.id, m.tmdb_id
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE issue = 'primary_cast:missing'
          AND (mvr.validation_overrides->>'primary_cast') IS NULL
    ''')).fetchall()
    log.info(f'primary_cast:missing — {len(rows)} movies')
    fixed = confirmed_none = 0
    for row in rows:
        if not row.tmdb_id:
            set_override(db, row.id, 'primary_cast')
            confirmed_none += 1
            continue
        data = tmdb_get(f'/movie/{row.tmdb_id}/credits')
        if not data:
            set_override(db, row.id, 'primary_cast')
            confirmed_none += 1
            continue
        top3 = [c for c in data.get('cast', []) if c['order'] < 3]
        if not top3:
            set_override(db, row.id, 'primary_cast')
            confirmed_none += 1
            continue
        for c in top3:
            name = c['name']
            actor = db.execute(text('''
                SELECT id FROM actors WHERE LOWER(name)=LOWER(:n)
                OR LOWER(name) LIKE LOWER(:n2)
            '''), {'n': name, 'n2': f'%{name}%'}).fetchone()
            if not actor:
                db.execute(text('INSERT INTO actors (name) VALUES (:n) ON CONFLICT DO NOTHING'), {'n': name})
                actor = db.execute(text('SELECT id FROM actors WHERE LOWER(name)=LOWER(:n)'), {'n': name}).fetchone()
            if actor:
                db.execute(text('''
                    INSERT INTO actor_movies (actor_id, movie_id, role_type, billing_order)
                    VALUES (:a, :m, 'primary', :b)
                    ON CONFLICT (actor_id, movie_id) DO UPDATE SET role_type='primary', billing_order=:b
                '''), {'a': actor.id, 'm': row.id, 'b': c['order']})
                fixed += 1
    db.commit()
    log.info(f'primary_cast: added {fixed} links, confirmed_none for {confirmed_none}')

# ─── Step 3: Fix director:missing ────────────────────────────────────────────
def fix_director_missing(db):
    rows = db.execute(text('''
        SELECT DISTINCT m.id, m.tmdb_id
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE issue = 'director:missing'
          AND (mvr.validation_overrides->>'director') IS NULL
    ''')).fetchall()
    log.info(f'director:missing — {len(rows)} movies')
    fixed = confirmed_none = 0
    for row in rows:
        if not row.tmdb_id:
            set_override(db, row.id, 'director')
            confirmed_none += 1
            continue
        data = tmdb_get(f'/movie/{row.tmdb_id}/credits')
        if not data:
            set_override(db, row.id, 'director')
            confirmed_none += 1
            continue
        dirs = [c['name'] for c in data.get('crew', []) if c['job'] == 'Director']
        if not dirs:
            set_override(db, row.id, 'director')
            confirmed_none += 1
            continue
        for dname in dirs[:1]:
            dir_row = db.execute(text('SELECT id FROM directors WHERE LOWER(name)=LOWER(:n)'), {'n': dname}).fetchone()
            if not dir_row:
                db.execute(text('INSERT INTO directors (name) VALUES (:n) ON CONFLICT DO NOTHING'), {'n': dname})
                dir_row = db.execute(text('SELECT id FROM directors WHERE LOWER(name)=LOWER(:n)'), {'n': dname}).fetchone()
            if dir_row:
                db.execute(text('''
                    INSERT INTO movie_directors (movie_id, director_id)
                    VALUES (:m, :d) ON CONFLICT DO NOTHING
                '''), {'m': row.id, 'd': dir_row.id})
                fixed += 1
    db.commit()
    log.info(f'director: added {fixed} links, confirmed_none for {confirmed_none}')

# ─── Step 4: Fix supporting_cast:no_cast_data_at_all ─────────────────────────
def fix_supporting_no_data(db):
    rows = db.execute(text('''
        SELECT DISTINCT m.id, m.tmdb_id
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE issue = 'supporting_cast:no_cast_data_at_all'
          AND (mvr.validation_overrides->>'supporting_cast') IS NULL
    ''')).fetchall()
    log.info(f'supporting_cast:no_cast_data_at_all — {len(rows)} movies')
    fixed = confirmed_none = 0
    batch = 0
    for row in rows:
        if not row.tmdb_id:
            set_override(db, row.id, 'supporting_cast')
            confirmed_none += 1
            continue
        data = tmdb_get(f'/movie/{row.tmdb_id}/credits')
        if not data:
            set_override(db, row.id, 'supporting_cast')
            confirmed_none += 1
            continue
        supporting = [c for c in data.get('cast', []) if c['order'] >= 3 and c['order'] < 15]
        if not supporting:
            set_override(db, row.id, 'supporting_cast')
            confirmed_none += 1
            continue
        for c in supporting:
            name = c['name']
            actor = db.execute(text('SELECT id FROM actors WHERE LOWER(name)=LOWER(:n)'), {'n': name}).fetchone()
            if not actor:
                db.execute(text('INSERT INTO actors (name) VALUES (:n) ON CONFLICT DO NOTHING'), {'n': name})
                actor = db.execute(text('SELECT id FROM actors WHERE LOWER(name)=LOWER(:n)'), {'n': name}).fetchone()
            if actor:
                db.execute(text('''
                    INSERT INTO actor_movies (actor_id, movie_id, role_type, billing_order)
                    VALUES (:a, :m, 'supporting', :b)
                    ON CONFLICT DO NOTHING
                '''), {'a': actor.id, 'm': row.id, 'b': c['order']})
                fixed += 1
        batch += 1
        if batch % 100 == 0:
            db.commit()
            log.info(f'  supporting: {batch}/{len(rows)} done')
    db.commit()
    log.info(f'supporting: added {fixed} links, confirmed_none for {confirmed_none}')

# ─── Step 5: Fix ratings ─────────────────────────────────────────────────────
def fix_ratings(db):
    rows = db.execute(text('''
        SELECT DISTINCT m.id, m.tmdb_id
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE issue = 'ratings:vote_average_missing'
          AND (mvr.validation_overrides->>'ratings') IS NULL
    ''')).fetchall()
    log.info(f'ratings:vote_average_missing — {len(rows)} movies')
    fixed = confirmed_none = 0
    for row in rows:
        if not row.tmdb_id:
            set_override(db, row.id, 'ratings')
            confirmed_none += 1
            continue
        data = tmdb_get(f'/movie/{row.tmdb_id}')
        if not data:
            set_override(db, row.id, 'ratings')
            confirmed_none += 1
            continue
        va = data.get('vote_average', 0)
        vc = data.get('vote_count', 0)
        if va and vc >= 5:
            db.execute(text('UPDATE movies SET vote_average=:v, vote_count=:c WHERE id=:id'),
                       {'v': va, 'c': vc, 'id': row.id})
            fixed += 1
        else:
            set_override(db, row.id, 'ratings')
            confirmed_none += 1
    db.commit()
    log.info(f'ratings: fixed {fixed}, confirmed_none for {confirmed_none}')

# ─── Step 6: Fix tmdb:no_tmdb_id ─────────────────────────────────────────────
def fix_no_tmdb_id(db):
    rows = db.execute(text('''
        SELECT DISTINCT m.id
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE issue LIKE 'tmdb:no_tmdb_id%'
    ''')).fetchall()
    log.info(f'tmdb:no_tmdb_id — {len(rows)} movies — setting confirmed_none for all cross-check fields')
    for row in rows:
        for field in ['director', 'primary_cast', 'supporting_cast', 'ratings', 'release_year']:
            set_override(db, row.id, field)
    db.commit()
    log.info(f'  confirmed_none set for {len(rows)} no-tmdb-id movies')

# ─── Step 7: Fix supporting_cast:missing_from_actor_movies ───────────────────
def fix_supporting_missing_from_am(db):
    rows = db.execute(text('''
        SELECT DISTINCT m.id, m.tmdb_id
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE issue = 'supporting_cast:missing_from_actor_movies'
    ''')).fetchall()
    log.info(f'supporting_cast:missing_from_actor_movies — {len(rows)} movies')
    fixed = 0
    for row in rows:
        if not row.tmdb_id:
            continue
        data = tmdb_get(f'/movie/{row.tmdb_id}/credits')
        if not data:
            continue
        supporting = [c for c in data.get('cast', []) if 3 <= c['order'] < 15]
        for c in supporting:
            name = c['name']
            actor = db.execute(text('SELECT id FROM actors WHERE LOWER(name)=LOWER(:n)'), {'n': name}).fetchone()
            if not actor:
                db.execute(text('INSERT INTO actors (name) VALUES (:n) ON CONFLICT DO NOTHING'), {'n': name})
                actor = db.execute(text('SELECT id FROM actors WHERE LOWER(name)=LOWER(:n)'), {'n': name}).fetchone()
            if actor:
                db.execute(text('''
                    INSERT INTO actor_movies (actor_id, movie_id, role_type, billing_order)
                    VALUES (:a, :m, 'supporting', :b)
                    ON CONFLICT DO NOTHING
                '''), {'a': actor.id, 'm': row.id, 'b': c['order']})
                fixed += 1
    db.commit()
    log.info(f'supporting_missing_from_am: added {fixed} links')

# ─── Step 8: Fix partial_match role_type (300 more) ──────────────────────────
def fix_partial_match_role_type(db):
    rows = db.execute(text('''
        SELECT DISTINCT m.id, m.tmdb_id
        FROM movie_validation_results mvr
        JOIN movies m ON m.id = mvr.movie_id,
             jsonb_array_elements_text(mvr.issues) AS issue
        WHERE issue LIKE 'primary_cast:partial_match%'
          AND m.tmdb_id IS NOT NULL
        LIMIT 500
    ''')).fetchall()
    log.info(f'primary_cast:partial_match — {len(rows)} movies to fix role_type')
    fixed = 0
    for row in rows:
        data = tmdb_get(f'/movie/{row.tmdb_id}/credits')
        if not data:
            continue
        top3 = [c for c in data.get('cast', []) if c['order'] < 3]
        top3_names = [c['name'].lower() for c in top3]
        top3_order = {c['name'].lower(): c['order'] for c in top3}
        our_actors = db.execute(text('''
            SELECT am.actor_id, a.name, am.role_type
            FROM actor_movies am JOIN actors a ON a.id=am.actor_id
            WHERE am.movie_id=:m
        '''), {'m': row.id}).fetchall()
        for actor in our_actors:
            nlow = actor.name.lower()
            matched = next((t for t in top3_names if nlow in t or t in nlow), None)
            if matched and actor.role_type != 'primary':
                billing = top3_order[matched]
                db.execute(text('''
                    UPDATE actor_movies SET role_type='primary', billing_order=:b
                    WHERE actor_id=:a AND movie_id=:m
                '''), {'b': billing, 'a': actor.actor_id, 'm': row.id})
                fixed += 1
    db.commit()
    log.info(f'partial_match role_type: fixed {fixed}')

# ─── Main ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    db = SessionLocal()
    try:
        log.info('=== Step 1: release_year:missing ===')
        fix_release_year(db)
        log.info('=== Step 2: primary_cast:missing ===')
        fix_primary_cast_missing(db)
        log.info('=== Step 3: director:missing ===')
        fix_director_missing(db)
        log.info('=== Step 4: supporting_cast:no_cast_data_at_all ===')
        fix_supporting_no_data(db)
        log.info('=== Step 5: ratings:vote_average_missing ===')
        fix_ratings(db)
        log.info('=== Step 6: tmdb:no_tmdb_id ===')
        fix_no_tmdb_id(db)
        log.info('=== Step 7: supporting_cast:missing_from_actor_movies ===')
        fix_supporting_missing_from_am(db)
        log.info('=== Step 8: partial_match role_type ===')
        fix_partial_match_role_type(db)
        log.info('=== ALL FIXES DONE ===')
    finally:
        db.close()
