https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html?highlight=docker#docker-compose-yaml

cat *.data|sed 's/T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]+0300//'| clickhouse-client --query="INSERT INTO event FORMAT JSONEachRow"

