Q5: Есть таблица фактов FactTable. Она содержит 100 миллионов записей, а также поле application_id.
И есть таблица измерений DimApplication с таким же полем application_id.
Кол-во записей в DimApplication – 100. application_id – уникальные.
Запрос первый после создания сессии с дефолтовыми параметрами:

    CREATE TABLE Result
    AS
    SELECT *
    FROM FactTable f
    LEFT JOIN DimApplication a ON f. application_id = a. application_id

Сколько будет файлов с данными в каталоге Hdfs для таблицы Result если при джоине случится shuffle?

Ответ: 4 file_size = 4GB, Parquet block size is 128 MB