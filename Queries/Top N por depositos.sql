with
pnl_table as (
select 
	user_id,
    sum(gross_pnl)*-1 as pnl
from trades
where position_created BETWEEN '2025-03-26' AND '2025-05-26' 
group by user_id
),
deposits_table as (
	select 
		user_id,
		sum(case when transaction_type = 'deposit' then 1 else 0 end) as qtd_depositos,
		sum(case when transaction_type = 'withdrawal' then 1 else 0 end) as qtd_saques,
		sum(case when transaction_type = 'deposit' then transaction_amount else 0 end) as total_depositado,
		sum(case when transaction_type = 'withdrawal' then transaction_amount else 0 end) as total_sacado
	from deposits
	where transaction_created BETWEEN '2025-03-26' AND '2025-05-26'
	group by user_id
)

select
	u.user_id as 'ID Usuário',
    u.aff as 'ID Afiliado',
	u.registered as 'Data de Cadastro',
    u.email as 'E-mail',
	d.qtd_depositos 'Qtd de Depósitos',
    d.total_depositado as 'Total Depositado',
    d.qtd_saques as 'Qtd de Saques',
    d.total_sacado as 'Total Sacado',
    pnl.pnl as 'PNL'
from users u
join pnl_table pnl on u.user_id = pnl.user_id
join deposits_table d on u.user_id = d.user_id
order by d.total_depositado