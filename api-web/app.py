
def get_secret(env_var: str, file_env_var: str) -> str:
    import os
    v = os.getenv(env_var)
    if v:
        return v
    f = os.getenv(file_env_var)
    if f:
        try:
            with open(f, "r", encoding="utf-8") as fh:
                return fh.read().strip()
        except OSError:
            pass
    raise RuntimeError(f"{env_var} no está definida en el entorno ni en {file_env_var}.")
import os
from flask import Flask, render_template, abort
import psycopg2
import psycopg2.extras
import requests

app = Flask(__name__)

def get_db_conn():
    # NADA hardcodeado: todo viene del entorno
    host = os.getenv("DB_HOST", "db")
    port = int(os.getenv("DB_PORT", "5432"))
    dbname = os.getenv("POSTGRES_DB", "moviesdb")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = get_secret("POSTGRES_PASSWORD","POSTGRES_PASSWORD_FILE")  # debe existir en el entorno

    if not password:
        raise RuntimeError("POSTGRES_PASSWORD no está definida en POSTGRES_PASSWORD ni en POSTGRES_PASSWORD_FILE.")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )

def query_one(sql, params=None):
    with get_db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
            return row

def query_all(sql, params=None):
    with get_db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            return rows

@app.route("/")
def home():
    movies_count = query_one("SELECT COUNT(*) AS c FROM movies")["c"]
    ratings_count = query_one("SELECT COUNT(*) AS c FROM ratings")["c"]
    return render_template("home.html", movies_count=movies_count, ratings_count=ratings_count)

@app.route("/movies")
def movies():
    # 100 películas con más valoraciones
    rows = query_all("""
        SELECT m.movie_id, m.title, m.year, m.genres,
               COUNT(r.rating) AS num_ratings,
               AVG(r.rating) AS avg_rating
        FROM movies m
        JOIN ratings r ON r.movie_id = m.movie_id
        GROUP BY m.movie_id
        ORDER BY num_ratings DESC
        LIMIT 100;
    """)
    return render_template("movies.html", movies=rows)

@app.route("/top")
def top():
    # Top 50 por media con al menos 50 votos
    rows = query_all("""
        SELECT m.movie_id, m.title, m.year, m.genres,
               COUNT(r.rating) AS num_ratings,
               AVG(r.rating) AS avg_rating
        FROM movies m
        JOIN ratings r ON r.movie_id = m.movie_id
        GROUP BY m.movie_id
        HAVING COUNT(r.rating) >= 50
        ORDER BY avg_rating DESC, num_ratings DESC
        LIMIT 50;
    """)
    return render_template("top.html", movies=rows)

@app.route("/movies/<int:movie_id>")
def movie_detail(movie_id: int):
    movie = query_one("""
        SELECT m.movie_id, m.title, m.year, m.genres,
               COUNT(r.rating) AS num_ratings,
               AVG(r.rating) AS avg_rating
        FROM movies m
        JOIN ratings r ON r.movie_id = m.movie_id
        WHERE m.movie_id = %s
        GROUP BY m.movie_id;
    """, (movie_id,))

    if not movie:
        abort(404)

    # Pedir recomendaciones al microservicio
    recs = []
    try:
        rec_url = f"http://recommender:5001/recommendations/movie/{movie_id}"
        resp = requests.get(rec_url, timeout=2)
        if resp.ok:
            recs = resp.json().get("recommendations", [])
    except Exception:
        recs = []

    return render_template("movie_detail.html", movie=movie, recommendations=recs)

if __name__ == "__main__":
	port = os.getenv("API_WEB_PORT", "5000")
	if "://" in port:
    		port = port.rsplit(":", 1)[-1]
	app.run(host="0.0.0.0", port=int(port))

