CREATE USER superset WITH PASSWORD 'superset';
CREATE DATABASE superset_db OWNER superset;
GRANT ALL PRIVILEGES ON DATABASE superset_db TO superset;
