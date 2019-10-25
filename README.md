# Glue together MQTT and TimescaleDB

Add a `.env` file with the configuration needed to connect to your TimescaleDB (PostgreSQL) instance. For example:
```
PGHOST=192.168.1.1
PGUSER=postgres
PGPASSWORD=password
PGDATABASE=mqtt_database
PGPORT=5432
```
