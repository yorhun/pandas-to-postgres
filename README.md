### Docker containers to load and query csv by using pandas chunk reading, insertion to postgres db, and pgadmin 

1. The csv to be inserted needs to be placed in /app/csv_file_name.csv
2. .env file in the same directory level as docker-compose.yaml needs to store the variables like usernames, passwords and ports in docker-compose for postgres and pgadmin
3. A file called /app/config.json similarly needs to store postgres variables to make a connection to the db
