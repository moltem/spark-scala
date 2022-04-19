Q6: При каких операциях возможен перекос данных ?

Ответ: При операциях, когд происходит Shufful

Spark shuffling вызывается при операциях трансформации:
    gropByKey(), reducebyKey(), join(), union(), groupBy() e.t.c

Spark RDD, shuffling вызывается при:
    like repartition(), coalesce(),  groupByKey(),  reduceByKey(), cogroup() and join() but not countByKey().

Spark SQL DataFrame API, shuffling вызывается при: 
    increases the partitions when the transformation operation performs shuffling. 
    DataFrame operations that trigger shufflings are join(), union() and all aggregate functions.

Shuffle partition size:
    Based on your dataset size, number of cores, and memory, Spark shuffling can benefit or harm your jobs. 
    When you dealing with less amount of data, you should typically reduce the shuffle partitions otherwise you will 
    end up with many partitioned files with a fewer number of records in each partition. which results in running many 
    tasks with lesser data to process. 
    On other hand, when you have too much of data and having less number of partitions results in fewer longer 
    running tasks and some times you may also get out of memory error.
    Getting a right size of the shuffle partition is always tricky and takes many runs with different value to achieve 
    the optimized number. This is one of the key property to look for when you have performance issues on Spark jobs.

Spark Default Shuffle Partition
    DataFrame increases the partition number to 200 automatically when Spark operation 
    performs data shuffling (join(), union(), aggregation functions). This default shuffle partition number comes from 
    Spark SQL configuration <strong>spark.sql.shuffle.partitions</strong> which is by default set to <strong>200</strong>.
    You can change this default shuffle partition value using conf method of the SparkSession object 
    or using Spark Submit Command Configurations.

--- | ---

--- | ---
|approx_count_distinct(e: Column)|	Returns the count of distinct items in a group.
--- | ---
|approx_count_distinct(e: Column, rsd: Double)|	Returns the count of distinct items in a group.
--- | ---
|avg(e: Column)|	Returns the average of values in the input column.
--- | ---
|collect_list(e: Column)|	Returns all values from an input column with duplicates.
--- | ---
|collect_set(e: Column)|	Returns all values from an input column with duplicate values .eliminated.
--- | ---
|corr(column1: Column, column2: Column)|	Returns the Pearson Correlation Coefficient for two columns.
--- | ---
|count(e: Column)|	Returns number of elements in a column.
--- | ---
|countDistinct(expr: Column, exprs: Column*)|	Returns number of distinct elements in the columns.
--- | ---
|covar_pop(column1: Column, column2: Column)|	Returns the population covariance for two columns.
--- | ---
|covar_samp(column1: Column, column2: Column)|	Returns the sample covariance for two columns.
--- | ---
|first(e: Column, ignoreNulls: Boolean)|	Returns the first element in a column when ignoreNulls is set to true, it returns first non null element.
--- | ---
|first(e: Column)|: Column	Returns the first element in a column.
--- | ---
|grouping(e: Column)|	Indicates whether a specified column in a GROUP BY list is aggregated or not, returns 1 for aggregated or 0 for not aggregated in the result set.
--- | ---
|kurtosis(e: Column)|	Returns the kurtosis of the values in a group.
--- | ---
|last(e: Column, ignoreNulls: Boolean)|	Returns the last element in a column. when ignoreNulls is set to true, it returns last non null element.
--- | ---
|last(e: Column)|	Returns the last element in a column.
--- | ---
|max(e: Column)|	Returns the maximum value in a column.
--- | ---
|mean(e: Column)|	Alias for Avg. Returns the average of the values in a column.
--- | ---
|min(e: Column)|	Returns the minimum value in a column.
--- | ---
|skewness(e: Column)|	Returns the skewness of the values in a group.
--- | ---
|stddev(e: Column)|	alias for `stddev_samp`.
--- | ---
|stddev_samp(e: Column)|	Returns the sample standard deviation of values in a column.
--- | ---
|stddev_pop(e: Column)|	Returns the population standard deviation of the values in a column.
--- | ---
|sum(e: Column)	|Returns the sum of all values in a column.
--- | ---
|sumDistinct(e: Column)	|Returns the sum of all distinct values in a column.
--- | ---
|variance(e: Column)	|alias for `var_samp`.
--- | ---
|var_samp(e: Column)	|Returns the unbiased variance of the values in a column.
--- | ---
|var_pop(e: Column)	|returns the population variance of the values in a column.
--- | ---

Вертикальные линии обозначают столбцы.

| Таблицы       | Это                | Круто |
| ------------- |:------------------:| -----:|
| столбец 3     | выровнен вправо    | $1600 |
| столбец 2     | выровнен по центру |   $12 |
| зебра-строки  | прикольные         |    $1 |

Внешние вертикальные линии (|) не обязательны и нужны только, чтобы сам код Markdown выглядел красиво. Тот же код можно записать так:

Markdown | не такой | красивый
--- | --- | ---
*Но выводится* | `так же` | **клево**
1 | 2 | 3