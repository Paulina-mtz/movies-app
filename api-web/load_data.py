
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
import csv
import psycopg2

DATA_DIR = "/app/data"

def conn():
    host = os.getenv("DB_HOST", "db")
    port = int(os.getenv("DB_PORT", "5432"))
    dbname = os.getenv("POSTGRES_DB", "moviesdb")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = get_secret("POSTGRES_PASSWORD","POSTGRES_PASSWORD_FILE")

    if not password:
        raise RuntimeError("POSTGRES_PASSWORD no está definida en POSTGRES_PASSWORD ni en POSTGRES_PASSWORD_FILE.")

    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)

def load_movies(cur, path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            movie_id = int(row["movieId"])
            title = row["title"]
            genres = row["genres"]
            # sacar año si viene como "Title (1995)"
            year = None
            if title.endswith(")") and "(" in title:
                try:
                    year = int(title.rsplit("(", 1)[1].rstrip(")"))
                except Exception:
                    year = None
            cur.execute(
                "INSERT INTO movies(movie_id, title, genres, year) VALUES (%s,%s,%s,%s) ON CONFLICT (movie_id) DO NOTHING",
                (movie_id, title, genres, year)
            )

def load_ratings(cur, path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute(
                "INSERT INTO ratings(user_id, movie_id, rating, ts) VALUES (%s,%s,%s,%s)",
                (int(row["userId"]), int(row["movieId"]), float(row["rating"]), int(float(row["timestamp"])))
            )

def main():
    movies_csv = os.path.join("data", "movies.csv")
    ratings_csv = os.path.join("data", "ratings.csv")

    print("Starting load...")
    with conn() as c:
        with c.cursor() as cur:
            load_movies(cur, movies_csv)
            print("Movies loaded.")
            load_ratings(cur, ratings_csv)
            print("Ratings loaded.")
    print("Done.")

if __name__ == "__main__":
    main()
