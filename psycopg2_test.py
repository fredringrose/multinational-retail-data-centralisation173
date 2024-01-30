import psycopg2

# Database connection details
host: '<data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com>'
port: '5432'
database: 'postgres'
user: 'aicore_admin'
password: 'AiCore2022'

# Establish the connection
conn = psycopg2.connect(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password
)

# Perform database operations using the connection
# ...

# Close the connection when finished
conn.close()