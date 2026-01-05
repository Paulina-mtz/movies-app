

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
    raise RuntimeError(f"{env_var} no está definida en {env_var} ni en {file_env_var}.")
import os
from flask import Flask, jsonify
import psycopg2
import psycopg2.extras

app = Flask(__name__)

def conn():
    host = os.getenv("DB_HOST", "db")
    port = int(os.getenv("DB_PORT", "5432"))
    dbname = os.getenv("POSTGRES_DB", "moviesdb")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = get_secret("POSTGRES_PASSWORD","POSTGRES_PASSWORD_FILE")

    if not password:
        raise RuntimeError("POSTGRES_PASSWORD no está definida en POSTGRES_PASSWORD ni en POSTGRES_PASSWORD_FILE.")

    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )

@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "recommender"})

@app.route("/recommendations/movie/<int:movie_id>")
def recommend(movie_id):
    sql_movie = "SELECT genres FROM movies WHERE movie_id = %s"
    sql_recs = """
        SELECT m.movie_id, m.title, m.year, m.genres,
               COUNT(r.rating) AS num_ratings,
               AVG(r.rating) AS avg_rating
        FROM movies m
        JOIN ratings r ON r.movie_id = m.movie_id
        WHERE m.genres ILIKE %s AND m.movie_id <> %s
        GROUP BY m.movie_id
        HAVING COUNT(r.rating) >= 50
        ORDER BY avg_rating DESC
        LIMIT 10;
    """

    with conn() as c:
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql_movie, (movie_id,))
            base = cur.fetchone()
            if not base:
                return jsonify({"recommendations": []})

            genre = base["genres"].split("|")[0]
            cur.execute(sql_recs, (f"%{genre}%", movie_id))
            recs = cur.fetchall()

    return jsonify({
        "base_movie_id": movie_id,
        "base_genre": genre,
        "recommendations": recs
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("APP_PORT", "5001")))

