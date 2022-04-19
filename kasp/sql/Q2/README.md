Как бы вы оптимизировали следующий запрос (показан полный скрипт таблицы; приведите обоснование своего выбора)?
create table dbo.detections (
    id int identity	primary key clustered
,	verdict varchar(64)	not null
,	detection_date  datetime    not null
,	subsystem   varchar(32)	not null
,	os_version  varchar(128)    not null
,	detections_cnt  int	not null
)

select *
from dbo.detections
where verdict = @a 
and detection_date > @b 
and subsystem = @c 

