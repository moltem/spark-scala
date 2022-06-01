#!/bin/bash
echo "==================     Help for psql   ========================="
echo "\\dt		: Describe the current database"
echo "\\d [table]	: Describe a table"
echo "\\c		: Connect to a database"
echo "\\h		: help with SQL commands"
echo "\\?		: help with psql commands"
echo "\\q		: quit"
echo "=================================================================="
docker exec -it postgres_playground psql -U docker -a -f ./docker-entrypoint-initdb.d/db.sql
docker exec -it postgres_playground psql -U docker -d posgreplayground