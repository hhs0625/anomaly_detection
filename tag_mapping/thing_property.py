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
    
    # Complex SQL query to fetch the desired information
    query = """
    SELECT s.ship_number,
           COUNT(DISTINCT dm.thing) AS unique_things,
           COUNT(DISTINCT dm.property) AS unique_properties,
           COUNT(dm.property) AS total_properties,
           most_used_thing.thing AS most_used_thing,
           most_used_thing.thing_count AS most_used_thing_count,
           most_used_property.property AS most_used_property,
           most_used_property.property_count AS most_used_property_count
    FROM data_mapping dm
    JOIN ships s ON dm.ships_idx = s.idx
    LEFT JOIN (
        SELECT ships_idx, thing, COUNT(thing) AS thing_count, RANK() OVER (PARTITION BY ships_idx ORDER BY COUNT(thing) DESC) AS rank
        FROM data_mapping
        GROUP BY ships_idx, thing
    ) most_used_thing ON dm.ships_idx = most_used_thing.ships_idx AND most_used_thing.rank = 1
    LEFT JOIN (
        SELECT ships_idx, property, COUNT(property) AS property_count, RANK() OVER (PARTITION BY ships_idx ORDER BY COUNT(property) DESC) AS rank
        FROM data_mapping
        GROUP BY ships_idx, property
    ) most_used_property ON dm.ships_idx = most_used_property.ships_idx AND most_used_property.rank = 1
    WHERE dm.ships_idx BETWEEN 1000 AND 1999
    GROUP BY s.ship_number, most_used_thing.thing, most_used_thing.thing_count, most_used_property.property, most_used_property.property_count
    ORDER BY s.ship_number;
    """
    cursor.execute(query)
    
    # Fetch the result
    results = cursor.fetchall()
    
    # Print the results
    print("ship_number | unique_things | unique_properties | total_properties | most_used_thing | most_used_thing_count | most_used_property | most_used_property_count")
    print("-" * 150)
    for row in results:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} | {row[6]} | {row[7]}")

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
