# Motivation

Makes DuckDB run as Postgresql Server.

Compatible with common PG tools and drivers:
- psql
- pgbench
- pg_dump/pg_restore
- psycopg2
- libpq/libpqxx
- PostgreSQL JDBC Driver
- pgx

# Build and run

Build in centos:
```
yum install boost-devel
git clone --recurse-submodules https://github.com/fanvanzh/PostDuck
cd PostDuck
cmake .
make -j 4
```

Run:
```
./postduck
```
