Как бы вы оптимизировали следующий запрос (показан полный скрипт таблицы; приведите обоснование своего выбора)?
create table dbo.detections (
    id int identity	primary key clustered
,	verdict varchar(64)	not null
,	detection_date  datetime    not null
,	subsystem   varchar(32)	not null
,	os_version  varchar(128)    not null
,	detections_cnt  int	not null
)

select 
    id,
    verdict,
    detection_date,
    subsystem,
    os_version,
    detections_cnt
from dbo.detections
where verdict = @a 
and subsystem = @c 
and detection_date > @b 

Оптимизация WHERE в запросе SELECT
Если where состоит из условий, объединенных AND,  они должны располагаться в порядке возрастания вероятности истинности данного условия. Чем быстрее мы получим false  в одном из условий - тем меньше условий будет обработано и тем быстрее выполняется запрос. 
