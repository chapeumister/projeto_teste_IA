import sqlite3
import os

def create_database():
    """
    Creates an SQLite database with tables for leagues, teams, matches, stats, and odds.
    The database file will be stored in the 'sports_prediction_ai/data' directory.
    """
    DB_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(DB_DIR, exist_ok=True)
    DB_PATH = os.path.join(DB_DIR, 'sports_data.db')

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Create Leagues Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS leagues (
                league_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                sport TEXT NOT NULL,
                country TEXT
            )
        ''')

        # Create Teams Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS teams (
                team_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                league_id INTEGER,
                country TEXT,
                FOREIGN KEY (league_id) REFERENCES leagues (league_id)
            )
        ''')

        # Create Matches Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS matches (
                match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                datetime DATETIME NOT NULL,
                home_team_id INTEGER NOT NULL,
                away_team_id INTEGER NOT NULL,
                league_id INTEGER,
                status TEXT,
                home_score INTEGER,
                away_score INTEGER,
                source TEXT,
                source_match_id TEXT,
                updated_at DATETIME,
                FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
                FOREIGN KEY (away_team_id) REFERENCES teams(team_id),
                FOREIGN KEY (league_id) REFERENCES leagues(league_id),
                UNIQUE (datetime, home_team_id, away_team_id, source)
            )
        ''')

        # Create Stats Table
        # Stores various statistics for matches or teams (can be flexible)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stats (
                stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER NOT NULL, -- Should be NOT NULL if it's a match stat
                stat_type TEXT NOT NULL, -- e.g., expected_goals, possession
                value_home TEXT, -- Using TEXT to accommodate various data types, can be NULL if stat is not team-specific or not applicable
                value_away TEXT, -- Using TEXT to accommodate various data types, can be NULL
                source TEXT, -- Source of the stat, e.g., FiveThirtyEight, Opta
                FOREIGN KEY (match_id) REFERENCES matches (match_id)
                -- Removed team_id as stats are per match, can be specified in stat_type if needed e.g. 'home_possession'
                -- Or, if team-specific stats not tied to a match are needed, a different table or nullable match_id might be better.
                -- For now, aligning with prompt's implied schema for FTE.
            )
        ''')

        # Create Odds Table
        # Stores betting odds from different bookmakers for matches
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS odds (
                odd_id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER NOT NULL,
                bookmaker TEXT NOT NULL,
                home_win_odds REAL,
                draw_odds REAL,
                away_win_odds REAL,
                last_updated TEXT,
                FOREIGN KEY (match_id) REFERENCES matches (match_id)
            )
        ''')

        conn.commit()
        print(f"Database created successfully at {DB_PATH}")

    except sqlite3.Error as e:
        print(f"Error creating database: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    create_database()
