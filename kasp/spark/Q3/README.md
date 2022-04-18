Q3. Есть 2 запроса, какой из них синтаксически не верный? Почему?
Ответ: Первый (Cannot resolve overloaded method 'filter' Должно быть "===")

=============== Error: Cannot resolve overloaded method 'filter' (===) ===============================================
    abtestInfo 
      .filter(
            upper(col("status")) == "APPLIED" 
      )
      .withColumn("period", lit(1))
    .show(100)
======================CORRECT===================================================================================
    abtestInfo
      .withColumn("period", lit(1))
      .filter(
        upper(col("status")).isin("APPLIED")
      )
    .show(100)



