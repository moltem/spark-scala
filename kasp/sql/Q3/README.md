Из таблицы следующей структуры:
create partition function pf_monthly(datetime) as
range right for values ('20100201', '20100301', '20100401', '20100501', '20100601', '20100701', '20100801', '20100901', '20101001', '20101101', '20101201')
go

create partition scheme ps_monthly as
partition pf_monthly
all to ([primary])
go

create table dbo.order_detail	(
		order_id		int			not null
	,	product_id		int			not null
	,	customer_id		int			not null
	,	purchase_date		datetime		not null	
	,	amount			money			not null
)
on ps_monthly(purchase_date)
go

create clustered index ix_purchase_date on dbo.order_detail(purchase_date)
go

Необходимо удалить случайно внесенные данные по клиенту с id 42, за период с мая по июнь (включительно) 2010-го года, что составляет более 80% записей за этот период. В таблице несколько миллиардов записей. Какие есть способы решения данной задачи?
