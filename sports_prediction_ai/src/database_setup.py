import sqlite3
import os

DATABASE_DIR = "sports_prediction_ai/database/"
DATABASE_FILE = os.path.join(DATABASE_DIR, "sports_data.sqlite")

def create_connection(db_file):
    """Create a database connection to a SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Successfully connected to SQLite database at {db_file}")
    except sqlite3.Error as e:
        print(e)
    return conn

def create_tables(conn):
    """Create tables in the SQLite database."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS leagues (
                league_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                sport TEXT NOT NULL,
                country TEXT,
                source_league_id TEXT,
                UNIQUE (name, sport)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS teams (
                team_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                short_name TEXT,
                sport TEXT NOT NULL,
                country TEXT,
                venue_name TEXT,
                source_team_id TEXT,
                UNIQUE (name, sport)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                league_id INTEGER,
                home_team_id INTEGER,
                away_team_id INTEGER,
                match_datetime_utc TEXT NOT NULL,
                status TEXT,
                home_score INTEGER,
                away_score INTEGER,
                winner TEXT,
                stage TEXT,
                matchday INTEGER,
                source_match_id TEXT NOT NULL,
                source_name TEXT NOT NULL,
                is_mock BOOLEAN DEFAULT 0,
                FOREIGN KEY (league_id) REFERENCES leagues (league_id),
                FOREIGN KEY (home_team_id) REFERENCES teams (team_id),
                FOREIGN KEY (away_team_id) REFERENCES teams (team_id),
                UNIQUE (source_match_id, source_name)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER,
                team_id INTEGER,
                player_id INTEGER,
                stat_type TEXT NOT NULL,
                stat_value TEXT NOT NULL,
                period TEXT,
                FOREIGN KEY (match_id) REFERENCES matches (match_id),
                FOREIGN KEY (team_id) REFERENCES teams (team_id),
                UNIQUE (match_id, team_id, stat_type, period)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS odds (
                odd_id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER,
                bookmaker TEXT NOT NULL,
                market_type TEXT NOT NULL,
                home_odds REAL,
                draw_odds REAL,
                away_odds REAL,
                other_odds_details TEXT,
                timestamp_utc TEXT,
                FOREIGN KEY (match_id) REFERENCES matches (match_id),
                UNIQUE (match_id, bookmaker, market_type)
            );
        """)

        # Add indexes for matches table
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_match_datetime_utc ON matches (match_datetime_utc);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_home_team_id ON matches (home_team_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_away_team_id ON matches (away_team_id);")
        
        conn.commit()
        print("Tables and indexes created successfully.")
    except sqlite3.Error as e:
        print(e)

def main():
    """Main function to create database and tables."""
    # Ensure the database directory exists
    os.makedirs(DATABASE_DIR, exist_ok=True)

    conn = create_connection(DATABASE_FILE)

    if conn is not None:
        create_tables(conn)
        conn.close()
        print("Database setup complete (tables and indexes).")
    else:
        print("Error! Cannot create the database connection.")

if __name__ == '__main__':
    main()
