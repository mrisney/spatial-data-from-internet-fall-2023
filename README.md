### Python DuckDB

#### Connect two tables based on a Geographic Reference

This script:

-   Loads the Lyme disease data and the population data into separate DuckDB tables.
-   Renames the first blank column in the population data to "State".
-   Performs a join on the "State" column.
-   Writes the combined data to a new CSV file.

Run this script, and it should join the two files based on the "State" column and add the 2016 population data to the Lyme incidences data. 

Here is an example command:

``` bash
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

# Rename the first blank column to 'State' and only keep the 2016 population column
con.execute("""
    ALTER TABLE population_data RENAME COLUMN " " TO State;
""")

# Perform the join
con.execute("""
    CREATE TABLE combined_data AS 
    SELECT lyme_incidences.*, population_data."2016" AS Population_2016
    FROM lyme_incidences
    JOIN population_data ON lyme_incidences.State = population_data.State
""")

# Write the joined data to a new CSV file
con.execute("COPY combined_data TO 'reported-lyme-incidences-2008-2016-with-population.csv' (FORMAT CSV)")

# Close the connection
con.close()

```

To install DuckDB with Python :

``` bash

pip install duckdb

```

https://duckdb.org/docs/api/python/overview.html
