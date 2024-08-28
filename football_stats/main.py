import os
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from config import DATABASE, GOOGLE_APPLICATION_CREDENTIALS
from langchain_google_vertexai import VertexAI

# Create the database URL
DATABASE_URL = f"postgresql+psycopg2://{DATABASE['user']}:{DATABASE['password']}@{DATABASE['host']}:{DATABASE['port']}/{DATABASE['name']}"

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Create a configured "Session" class
Session = sessionmaker(bind=engine)

def connect_to_db():
    """Create and return a new session."""
    session = Session()
    return session

                        ######## FETCH RECORD FROM DATABASE ########

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

def query_players_ai(session, query_ai):
    """Query the players table."""
    try:
        query = text(query_ai)
        result = session.execute(query).fetchall()
        # for row in result:
        #     print(row)
        return result
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

def get_player_by_id(session, player_id):
    """Retrieve a player and their additional info by player_id."""
    try:
        query = text("""
            SELECT p.*, pai.*
            FROM players p
            LEFT JOIN players_add_info pai ON p.player_id = pai.player_id
            WHERE p.player_id = :player_id
        """)
        result = session.execute(query, {'player_id': player_id}).fetchone()
        return result
    except Exception as e:
        print(f"Error retrieving player: {e}")
        return None

                        ######## INSERT RECORD TO DATABASE ########

def insert_player(session, player_data):
    """Insert a new record into the players table."""
    try:
        insert_stmt = text("""
            INSERT INTO players (first_name, last_name, age, nationality, position, height, weight, overall_rating, potential_rating, pace, shooting, passing, dribbling, defending, physical, created_at)
            VALUES (:first_name, :last_name, :age, :nationality, :position, :height, :weight, :overall_rating, :potential_rating, :pace, :shooting, :passing, :dribbling, :defending, :physical, :created_at)
            RETURNING player_id
        """)
        result = session.execute(insert_stmt, player_data)
        player_id = result.scalar()
        session.commit()
        print("Player record inserted successfully.")
        return player_id
    except Exception as e:
        print(f"Error inserting player record: {e}")
        session.rollback()
        return None

def insert_player_add_info(session, player_add_info_data):
    """Insert a new record into the players_add_info table."""
    try:
        insert_stmt = text("""
            INSERT INTO players_add_info (player_id, birthplace, current_club, club_join_date, contract_end_date, market_value, created_at)
            VALUES (:player_id, :birthplace, :current_club, :club_join_date, :contract_end_date, :market_value, :created_at)
        """)
        session.execute(insert_stmt, player_add_info_data)
        session.commit()
        print("Player additional info record inserted successfully.")
    except Exception as e:
        print(f"Error inserting player additional info record: {e}")
        session.rollback()

def add_player_and_info(session):

    # Example player data (excluding player_id)
    player_data = {
        'first_name': 'John',
        'last_name': 'Doe',
        'age': 25,
        'nationality': 'USA',
        'position': 'Midfielder',
        'height': 1.80,  # Use float values if your database expects it
        'weight': 75.00,  # Ensure the weight fits the expected precision
        'overall_rating': 85,
        'potential_rating': 90,
        'pace': 70,
        'shooting': 80,
        'passing': 85,
        'dribbling': 75,
        'defending': 60,
        'physical': 80,
        'created_at': '2024-08-04'
    }

    # Example player additional info data (excluding player_id)
    player_add_info_data = {
        'birthplace': 'New York',
        'current_club': 'NYC FC',
        'club_join_date': '2021-01-01',
        'contract_end_date': '2024-12-31',
        'market_value': 10000000.00,  # Ensure the market value fits the expected precision
        'created_at': '2024-08-04'
    }

    """Add a player and their additional info to the database."""

    # Insert player and retrieve player_id
    player_id = insert_player(session, player_data)
    
    if player_id:
        # Update player_add_info_data with the retrieved player_id
        player_add_info_data['player_id'] = player_id
        
        # Insert player additional info
        insert_player_add_info(session, player_add_info_data)

                        ######## VERTEXAI PART ########

def use_vertexai_for_nlp(input_text, session):
    """Use VertexAI to process natural language input and generate a response."""
    llm = VertexAI()

    messages = [
        (
            "system",
            "After the descriptions you will see user prompt to take an ACTION, just type SQL query as plain text without any other text or sign. The following bullet points are the concise description for each column in players and players_add_info table. If you can't find a column in a table you checked, you can always check the other table. You can join players and players_add_info tables together using player_id. Here are the bullet points for players table: 1. player_id: (table name: players) A unique identifier for each player. 2. first_name: (table name: players) The name of the player. 3. last_name: (table name: players) The last name of player. 4. age: (table name: players) Age of player. 5.  nationality: (table name: players) A string identifier representing the player's nationality. 6. position: (table name: players) The position of player where they play at football pitch",
        ),
        ("human", input_text),
    ]

    ai_msg = llm.invoke(messages)
    result = query_players_ai(session, ai_msg)

    messages2 = [
        (
            "system",
            "Convert list as JSON format.",
        ),
        ("human", result),
    ]

    ai_msg2 = llm.invoke(messages2)

    return ai_msg2

def main():
    player_id = 48
    session = connect_to_db()

    # # Query players table
    # query_players(session, player_id)
    # # Query players_add_info table
    # query_players_add_info(session, player_id)
    # # Query joined tables
    # query_joined_tables(session, player_id)
    # # Query select 5 column
    # query_and_print_results(session)

    #add_player_and_info(session)

    # # Example usage: retrieve a player by ID
    # player_id = 1  # Replace with the actual player_id you want to query
    # player = get_player_by_id(session, player_id)
    # print("Player:", player)

    # # Example usage: Use VertexAI for natural language processing
    input_text = "Provide players who is playing at Forward"
    ai_response = use_vertexai_for_nlp(input_text, session)
    print("AI Response:", ai_response)

    session.close()

if __name__ == '__main__':
    main()
