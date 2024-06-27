# Import necessary libraries
import psycopg2
import pysolr

# PostgreSQL connection details
pg_config = {
    'dbname': 'detail_data',
    'user': 'postgres',
    'password': 'Threadripperxeon6432',
    'host': 'localhost',
    'port': '5432'
}

table_name = 'fields_requested'

# Solr connection details
solr_url = 'http://localhost:8983/solr/' + table_name

# Function to fetch data from PostgreSQL
def fetch_data_from_postgresql():
    try:
        # Connect to PostgreSQL database
        connection = psycopg2.connect(**pg_config)
        cursor = connection.cursor()

        # Execute SQL query to fetch data
        cursor.execute("SELECT * FROM public." + table_name)
        records = cursor.fetchall()
        
        # Get column names from cursor
        colnames = [desc[0] for desc in cursor.description]
        #df = pandas.DataFrame(records, columns = colnames)

        # Convert records to list of dictionaries
        data = [dict(zip(colnames, record)) for record in records]

        # Close the cursor and connection
        cursor.close()
        connection.close()

        #print(data)

        return data

    except (Exception, psycopg2.Error) as error:
        print("Error fetching data from PostgreSQL", error)
        return None
    

def clean_data(data):
    for row in range(len(data)):
        for key, values in data[row].items(): 
            if values == "#":
                data[row].update({key: None})
            else: 
                data[row].update({key: str(values)})

# Function to index data to Solr
def index_data_to_solr(data):
    try:
        # Connect to Solr
        solr = pysolr.Solr(solr_url, always_commit=True)

        solr.delete(q='*:*')

        # Index data
        templist = []
        for row in range(len(data)):
            templist.append(data[row])
            try:
                solr.add(templist)
            except Exception as e:
                print(e, str(row))
            templist.clear()

        #solr.add(data)
        print("Data indexed successfully to Solr")

    except Exception as error:
        print("Error indexing data to Solr", error)

# Main function to execute the process
def main():
    # Fetch data from PostgreSQL
    data = fetch_data_from_postgresql()
 
    clean_data(data)
    if data:
        # Index data to Solr
        index_data_to_solr(data)
    else:
        print("No data fetched from PostgreSQL")

# Execute main function
if __name__ == "__main__":
    main()
