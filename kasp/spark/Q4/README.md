Q4. Можно ли увидеть на физическом плане запроса операцию Hash Join на запросе:
   SELECT t1.ip
   FROM Table1 t1
   JOIN Table2 T2 on t1.ip >= t2.ip_from AND t2.ip <= t2.ip_to
   Обоснуйте ответ.

   Ответ: Нет, хэш работает только с "="
   Или если мы используем хинт broadcast(таблица)

