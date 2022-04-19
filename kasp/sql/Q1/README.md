Дана таблица:
create table dbo.detections (
virus_name		varchar(64) 	not null
,	detection_date 		datetime	not null
,	detections_cnt		int		not null
)
Требуется написать запрос, возвращающий для каждого названия вируса минимальную дату, когда количество обнаружений было максимально, и максимальную дату, когда количество обнаружений было минимально, а также количество обнаружений.

Ответ:

select
    virus_name,
   
    -- минимальную дату, когда количество обнаружений было максимально,
    first_value(detection_date)over(partiton by virus_name order by detections_cnt desc) min_dt_vrs
   
    -- максимальную дату, когда количество обнаружений было минимально,
    first_value(detection_date)over(partiton by virus_name order by detections_cnt asc) max_dt_vrs  
   
    -- количество обнаружений
    sum(detections_cnt) over(partition by virus_name) vrs_cnt
from
    dbo.detections;
