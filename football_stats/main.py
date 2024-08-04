import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from config import DATABASE

# Create the database URL
DATABASE_URL = f"postgresql+psycopg2://{DATABASE['user']}:{DATABASE['password']}@{DATABASE['host']}:{DATABASE['port']}/{DATABASE['name']}"

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Create a configured "Session" class
Session = sessionmaker(bind=engine)

def connect_to_db():
    """Create and return a new session."""
    session = Session()
    return session

def query_players(session, player_id):
    """Query the players table."""
    try:
        query = text("SELECT * FROM players WHERE player_id = :player_id")
        result = session.execute(query, {'player_id': player_id}).fetchall()
        print("Players table results:")
        for row in result:
            print(row)
    except Exception as e:
        print(f"Error executing query on players table: {e}")

def query_players_add_info(session, player_id):
    """Query the players_add_info table."""
    try:
        query = text("SELECT * FROM players_add_info WHERE player_id = :player_id")
        result = session.execute(query, {'player_id': player_id}).fetchall()
        print("Players_add_info table results:")
        for row in result:
            print(row)
    except Exception as e:
        print(f"Error executing query on players_add_info table: {e}")

def query_joined_tables(session, player_id):
    """Join players and players_add_info tables and query."""
    try:
        query = text("""
            SELECT p.*, pai.*
            FROM players p
            JOIN players_add_info pai ON p.player_id = pai.player_id
            WHERE p.player_id = :player_id
        """)
        result = session.execute(query, {'player_id': player_id}).fetchall()
        print("Joined table results:")
        for row in result:
            print(row)
    except Exception as e:
        print(f"Error executing join query: {e}")

def query_and_print_results(session):
    try:
        # Execute a query using the session
        result = session.execute(text("SELECT * FROM players LIMIT 5;")).fetchall()
        print("Query results:")
        for row in result:
            print(row)
    except Exception as e:
        print(f"Error executing query: {e}")
    finally:
        session.close()

def main():
    player_id = 48
    session = connect_to_db()

    # Query players table
    query_players(session, player_id)
    
    # Query players_add_info table
    query_players_add_info(session, player_id)
    
    # Query joined tables
    query_joined_tables(session, player_id)

    query_and_print_results(session)

if __name__ == '__main__':
    main()
