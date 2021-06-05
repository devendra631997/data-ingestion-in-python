import dask.dataframe
import time
import mysql.connector
import concurrent.futures
import traceback
start = time.perf_counter()

####### Read data from csv
data = dask.dataframe.read_csv('products.csv')
schema = data.columns
data10 = data.head(2).values.tolist() # get 10 rows and converted into list
######## End read data from csv

###########DATA BASE CONNECTION CALLS
mydb = mysql.connector.connect(
  host='localhost',
  user='dev',
  password='Devendra@123',
  database="mydatabase"
)
mycursor = mydb.cursor()
# mycursor.execute("CREATE DATABASE IF NOT EXISTS mydatabase")
mycursor.execute("DROP TABLE IF EXISTS products")
# name,sku,description
mycursor.execute("CREATE TABLE IF NOT EXISTS products (name VARCHAR(255), sku VARCHAR(255) PRIMARY KEY, description VARCHAR(255))") 


###############ends database connection calls

########### Multi processing
def Insert_query(row):
    print(row)
    sql = "INSERT INTO products (name,sku,description) VALUES (%s, %s, %s)"
    val = row
    try:
        mycursor.execute(sql, val)
    except:
        print(traceback.format_exc())
    print(f'Sleeping second(s)...')

with concurrent.futures.ThreadPoolExecutor() as executor:
    results = executor.map(Insert_query, data10)
############## Ends Multi processing
mydb.commit()

finish = time.perf_counter()

print(f'time taken=-===========>{finish-start}')
