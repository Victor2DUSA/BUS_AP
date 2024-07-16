import psycopg2
from psycopg2 import OperationalError

def create_connection(db_name, db_user, db_password, db_host, db_port):
    """ Create a database connection to the PostgreSQL database """
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        #print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

def execute_query(connection, query):
    """ Execute a single query """
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print(f"The error '{e}' occurred")
        return None
    
def format_time(time_value):
    """Format time value to a readable string"""
    return time_value.strftime('%I:%M %p')

def main():
    # Database connection parameters
    db_name = "BUSS_GOLDEN"
    db_user = "postgres"
    db_password = "123451"
    db_host = "127.0.0.1"  # typically 'localhost' or '127.0.0.1'
    db_port = "5432"  # typically '5432'

    # Create a database connection
    connection = create_connection(db_name, db_user, db_password, db_host, db_port)
    
    # Define the query
    query = """
    SELECT route_number, departure_time, arrival_time 
    FROM bus_schedule 
    WHERE starting_point = 'Retreat' 
    AND destination = 'Cape Town' 
    AND days_of_operation = 'Weekdays'
    AND departure_time BETWEEN '05:00' AND '06:00';
    """
    
    # Execute the query
    if connection:
        results = execute_query(connection, query)
        if results:
            formatted_results = [
                [ format_time(row[1]), format_time(row[2])] for row in results]
            print(formatted_results)
        connection.close()



if __name__ == "__main__":
    main()
