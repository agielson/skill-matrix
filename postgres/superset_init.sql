CREATE USER superset WITH PASSWORD 'superset';
CREATE DATABASE superset_db OWNER superset;
GRANT ALL PRIVILEGES ON DATABASE superset_db TO superset;

CREATE USER examples WITH PASSWORD 'examples';
CREATE DATABASE examples_db OWNER examples;
GRANT ALL PRIVILEGES ON DATABASE examples_db TO examples;