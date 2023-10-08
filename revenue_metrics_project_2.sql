--робимо сті для джойну 2 таблиць

with cte as (
	select
		CAST(date_trunc('month',
		gp.payment_date) as date) as payment_month,
		gpu.user_id as user_id,
		gpu.game_name as game_name,
		sum(gp.revenue_amount_usd) as revenue_amount,
		gpu."language" as language,
		gpu.has_older_device_model as has_older_device_model,
		gpu.age as age
	from
		project.games_paid_users gpu
	left join project.games_payments gp on
		gpu.user_id = gp.user_id
		and gpu.game_name = gp.game_name
	group by
		payment_month,
		gpu.user_id,
		gpu.game_name,
		gpu."language",
		gpu.has_older_device_model,
		gpu.age
	order by
		payment_month,
		gpu.user_id
		),
		


cte2 as (
	select
	payment_month,
	
	--віконна функція для пошуку найменшої дати (першої оплати для кожного юзера)	
	first_value (cte.payment_month) over (
	partition by cte.user_id 
	order by cte.payment_month) as date_first_payment,
	
	--віконна функція для пошуку попереднього платіжного місяця	
	lag(payment_month, 1) over (
	partition by user_id
	order by payment_month) as prev_payment_month,
	
	--віконна функція для пошуку наступного платіжного місяця
	lead(payment_month, 1) over (
	partition by user_id
	order by payment_month) as next_payment_month,
	
	cast (payment_month + interval '1 month' as date) as next_cal_month,
	
	cte.user_id,
	cte.game_name,
	cte.revenue_amount,
	
	--віконна функція для пошуку наступного ревенью
	lead(revenue_amount, 1) over (
	partition by user_id
	order by user_id) as next_revenue,
	
	
	
	cte.language,
	cte.has_older_device_model,
	cte.age
	
from
	cte
	
order by cte.payment_month
),


--сті для знаходження churn місяця
cte3 as (
	select
		cast (payment_month + interval '1 month' as date)  as payment_month,
		date_first_payment,
		prev_payment_month,
		next_payment_month,
		next_cal_month,
		user_id,
		game_name,
		-revenue_amount as revenue_amount,
		next_revenue,
		language,
		has_older_device_model,
		age
		
	from
		cte2
	where
		next_payment_month is null or next_payment_month <> next_cal_month
),

--сті для об'єднання головної сті і черн місяців
cte4 as (
	select * from cte2
	union all 
	select * from cte3
	order by payment_month
)


select
	payment_month,
--	date_first_payment,
--	prev_payment_month,
--	next_payment_month,
--	next_cal_month,
	user_id,
	
--	шукаємо NEW MRR, де місяць оплати = найменшому місяцю (першому) оплати для юзера
	case 
		when payment_month = 
	
		first_value (payment_month) over (
		partition by user_id 
		order by payment_month)
		
		then 'NEW MRR' 
		
--	шукаємо MRR де дата платежу - 1 місяць = попередня дата платежу, тобто повторюється з місяця в місяць		
		when (payment_month - interval '1 month') = prev_payment_month
		then 'MRR'
		
--	згідно з умовами сті, де шукали churn місяць, ревенью у churn місяці буде від'ємна 
		when revenue_amount < 0 
		then 'Churn'
		
	--	якщо дата платежу - 1 місяць більше ніж попередня дата оплати і ревенью не від'ємне, то це повернення з churn
		when (payment_month - interval '1 month') > prev_payment_month
		and revenue_amount > 0 
		then 'Back Churn'
	
	
	end type_of_revenue,
	
	game_name,
	revenue_amount,
	language,
	has_older_device_model,
	age
from
	cte4
	
	order by payment_month
	
	
