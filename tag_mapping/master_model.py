import psycopg2
from psycopg2 import Error

try:
    # Establishing the connection
    connection = psycopg2.connect(
        user="mna_lab",
        password="mnalab1!",
        host="hgs-aiins-rds.ce8fyfkmlsoh.ap-northeast-2.rds.amazonaws.com",
        port="5432",
        database="hipom"
    )
    cursor = connection.cursor()
    
    # Query to count unique things, unique properties, and total properties
    query_counts = """
    SELECT
        COUNT(DISTINCT thing) AS unique_thing_count,
        COUNT(DISTINCT property) AS unique_property_count,
        COUNT(property) AS total_property_count
    FROM master_data_model;
    """
    cursor.execute(query_counts)
    
    # Fetch the result
    result_counts = cursor.fetchone()
    print(f"Unique Thing Count: {result_counts[0]}")
    print(f"Unique Property Count: {result_counts[1]}")
    print(f"Total Property Count: {result_counts[2]}")
    
    print("----")
    
    # Query to get the list of unique things
    query_things = "SELECT DISTINCT thing FROM master_data_model ORDER BY thing;"
    cursor.execute(query_things)
    
    # Fetch and print the list of unique things
    things = cursor.fetchall()
    print("List of Unique Things:")
    for thing in things:
        print(thing[0])
    
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
