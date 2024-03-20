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
    
    # Combined query for matching and non-matching records count
    query_combined = """
    SELECT
        dm.ships_idx,
        COUNT(DISTINCT CASE WHEN mdm.thing IS NOT NULL AND mdm.property IS NOT NULL THEN CONCAT(dm.thing, '-', dm.property) END) AS match_count,
        COUNT(DISTINCT CASE WHEN mdm.thing IS NULL OR mdm.property IS NULL THEN CONCAT(dm.thing, '-', dm.property) END) AS non_match_count
    FROM
        data_mapping dm
    LEFT JOIN
        master_data_model mdm ON dm.thing = mdm.thing AND dm.property = mdm.property
    GROUP BY
        dm.ships_idx;
    """
    cursor.execute(query_combined)
    combined_results = cursor.fetchall()
    for record in combined_results:
        print(f"ships_idx: {record[0]}, Match Count: {record[1]}, Non-Match Count: {record[2]}")
    
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
