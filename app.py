# football_api/app.py
"""
Flask REST API for football statistics.
Stores scraped data in SQLite and exposes:
    POST /scrape       – trigger scraping
    GET  /players      – list all players
    GET  /players/<id> – single player
    GET  /rank         – weighted ranking
    GET  /status       – health / last-scrape info
"""

import os
import sqlite3
import json
import logging
from datetime import datetime
from contextlib import closing

from flask import Flask, jsonify, request, g
from flask_cors import CORS

# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ------------------------------------------------------------------
# Flask setup
# ------------------------------------------------------------------
app = Flask(__name__)
CORS(app)  # allow all origins – tighten in production
app.config["DATABASE"] = os.getenv("DB_PATH", "football_stats.db")

# ------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------
def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(
            app.config["DATABASE"],
            detect_types=sqlite3.PARSE_DECLTYPES,
        )
        g.db.row_factory = sqlite3.Row  # dict-like rows
    return g.db

def close_db(e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()

@app.teardown_appcontext
def _close_db(error):
    close_db()

def init_db():
    """Create tables if they don’t exist."""
    schema = """
    CREATE TABLE IF NOT EXISTS players (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        name         TEXT NOT NULL,
        squad        TEXT,
        age          INTEGER,
        position     TEXT,
        minutes      INTEGER,
        goals        INTEGER,
        assists      INTEGER,
        xg           REAL,
        xa           REAL,
        data         TEXT,               -- JSON blob with all metrics
        updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS meta (
        key   TEXT PRIMARY KEY,
        value TEXT
    );
    """
    with closing(get_db()) as db:
        db.executescript(schema)
        db.commit()

# ------------------------------------------------------------------
# Scraper integration
# ------------------------------------------------------------------
# We reuse the user-supplied code by importing it as a module.
# Place the scraping code in `scraper.py` (same folder) and expose
# a thin wrapper so we can call it without the CLI `main()`.

from scraper import (
    scrape_fbref_stats,
    scrape_whoscored_stats,
    scrape_understat_stats,
    merge_data_sources,
    calculate_derived_metrics,
)

DRIVER = None  # lazy init inside scrape task

def get_driver():
    global DRIVER
    if DRIVER is None:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        opts = Options()
        for arg in ("--headless", "--disable-gpu", "--window-size=1920,1080"):
            opts.add_argument(arg)
        DRIVER = webdriver.Chrome(options=opts)
    return DRIVER

def scrape_all():
    """Run full scraping pipeline and return DataFrame."""
    logging.info("Starting scrape job")

    fbref = scrape_fbref_stats(
        "https://fbref.com/en/comps/9/Premier-League-Stats", "Premier-League"
    )
    whoscored = scrape_whoscored_stats(
        "https://www.whoscored.com/Regions/252/Tournaments/2/Seasons/9019/Stages/21135/PlayerStatistics/England-Premier-League-2022-2023"
    )
    understat = scrape_understat_stats("https://understat.com/league/EPL")

    merged = merge_data_sources(fbref, whoscored, understat)
    if merged is None:
        raise RuntimeError("Merging data sources failed")
    final = calculate_derived_metrics(merged)
    logging.info("Scraping finished – %s players", len(final))
    return final

# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------
@app.route("/status", methods=["GET"])
def status():
    db = get_db()
    row = db.execute("SELECT value FROM meta WHERE key='last_scrape'").fetchone()
    last_scrape = row["value"] if row else None
    return jsonify({"last_scrape": last_scrape, "status": "ok"})

@app.route("/scrape", methods=["POST"])
def trigger_scrape():
    """Scrape fresh data and persist to SQLite."""
    try:
        df = scrape_all()
    except Exception as exc:
        logging.exception("Scrape failed")
        return jsonify({"error": str(exc)}), 500

    # Upsert into DB
    db = get_db()
    players = df.to_dict(orient="records")
    for p in players:
        db.execute(
            """INSERT INTO players(name, squad, age, position, minutes,
                                   goals, assists, xg, xa, data)
               VALUES (?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(name, squad) DO UPDATE SET
                   age=excluded.age,
                   minutes=excluded.minutes,
                   goals=excluded.goals,
                   assists=excluded.assists,
                   xg=excluded.xg,
                   xa=excluded.xa,
                   data=excluded.data,
                   updated_at=CURRENT_TIMESTAMP;
            """,
            (
                p.get("Player"),
                p.get("Squad"),
                int(p.get("Age", 0)) if str(p.get("Age")).isdigit() else None,
                p.get("Pos"),
                int(p.get("Min", 0)) if str(p.get("Min")).isdigit() else None,
                int(p.get("Goals", 0)) if str(p.get("Goals")).isdigit() else None,
                int(p.get("Assists", 0)) if str(p.get("Assists")).isdigit() else None,
                float(p.get("xG", 0)) if p.get("xG") else None,
                float(p.get("xA", 0)) if p.get("xA") else None,
                json.dumps(p),  # full metrics as JSON
            ),
        )
    db.execute(
        "INSERT OR REPLACE INTO meta(key,value) VALUES (?,?)",
        ("last_scrape", datetime.utcnow().isoformat()),
    )
    db.commit()
    return jsonify({"players_inserted": len(players)})

@app.route("/players", methods=["GET"])
def list_players():
    db = get_db()
    rows = db.execute(
        "SELECT id, name, squad, age, position, minutes, goals, assists FROM players"
    ).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/players/<int:player_id>", methods=["GET"])
def get_player(player_id):
    db = get_db()
    row = db.execute(
        "SELECT * FROM players WHERE id=?", (player_id,)
    ).fetchone()
    if not row:
        return jsonify({"error": "Not found"}), 404
    player = dict(row)
    player["data"] = json.loads(player["data"]) if player["data"] else {}
    return jsonify(player)

@app.route("/rank", methods=["GET"])
def rank_players():
    """
    Simple weighted ranking:
        goals*4 + assists*3 + minutes*0.01 + (xg+xa)*2
    Accepts ?top=N (default 20)
    """
    top = request.args.get("top", type=int, default=20)
    db = get_db()
    rows = db.execute(
        """
        SELECT id, name, squad, position,
               goals, assists, minutes, xg, xa
        FROM players
        WHERE minutes > 0
        ORDER BY (goals*4 + assists*3 + minutes*0.01 + (IFNULL(xg,0)+IFNULL(xa,0))*2) DESC
        LIMIT ?
        """,
        (top,),
    ).fetchall()
    return jsonify([dict(r) for r in rows])

# ------------------------------------------------------------------
# CLI helper
# ------------------------------------------------------------------
@app.cli.command("init-db")
def init_db_command():
    init_db()
    print("Database initialised.")

# ------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------
if __name__ == "__main__":
    init_db()  # ensure tables exist
    app.run(debug=True, port=5000)
