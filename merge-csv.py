import duckdb

# Connect to an in-memory DuckDB database
con = duckdb.connect(database=':memory:')

# Load the Lyme incidences CSV into a DuckDB table
con.execute("""
    CREATE TABLE lyme_incidences AS 
    SELECT * 
    FROM read_csv_auto('reported-lyme-incidences-2008-2016.csv')
""")

# Load the population data CSV into another DuckDB table, skipping the first 4 rows
con.execute("""
    CREATE TABLE population_data AS 
    SELECT * 
    FROM read_csv_auto('nst-est2016-01.csv', HEADER=True, SKIP=4)
""")

# Perform the join using 'column0' as the state identifier in the population data and sort the result alphabetically by state
con.execute("""
    CREATE TABLE combined_data AS 
    SELECT lyme_incidences.*, population_data."2016" AS Population_2016
    FROM lyme_incidences
    JOIN population_data ON lyme_incidences.State = population_data.column0
    ORDER BY lyme_incidences.State
""")

# Write the sorted joined data to a new CSV file
con.execute("COPY combined_data TO 'reported-lyme-incidences-2008-2016-with-population.csv' (FORMAT CSV)")

# Close the connection
con.close()
