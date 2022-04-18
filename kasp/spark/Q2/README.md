Q2. Сколько партиций по умолчанию имеет DataFrame при чтении из JDBC источника? 
Ответ: 1

Partitioning with JDBC sources (https://medium.com/@radek.strnad/tips-for-using-jdbc-in-apache-spark-sql-396ea7b2e3d3)

Traditional SQL databases can not process a huge amount of data on different nodes as a spark. In order to allow a
Spark to read data from a database via JDBC in parallel, you must specify the level of parallel reads/writes which is
controlled by the following option .option('numPartitions', parallelismLevel). The specified number controls a maximal
number of concurrent JDBC connections. By default, you read data to a single partition which usually doesn't fully
utilize your SQL database and obviously Spark.

something like
SELECT * FROM pets WHERE owner_id >= 1 and owner_id < 1000
SELECT * FROM pets WHERE owner_id >= 1000 and owner_id < 2000
SELECT * FROM pets WHERE owner_id >= 2000 and owner_id < 3000

