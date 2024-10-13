### Docker containers to chunk load csv to db with a db manager accesible via browser
<img width="832" alt="csv-to-db-diagram" src="https://github.com/user-attachments/assets/4165bdfd-88f0-4fd9-8b85-aa438176819d">

1. The csv to be inserted needs to be placed in /app/csv_file_name.csv
2. .env file in the same directory level as docker-compose.yaml needs to store the variables like usernames, passwords and ports in docker-compose for postgres and pgadmin
3. A file called /app/config.json similarly needs to store postgres variables to make a connection to the db
