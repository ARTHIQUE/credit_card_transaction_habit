create database credit_card_transaction;


select * from credit_card_transcations;


-- 1. write a query to print top 5 cities with highest spends and 
-- their percentage contribution of total credit card spends.


with cte as(
select city,sum(amount)as total_spends
from credit_card_transcations
group by city
order by sum(amount) desc
)
select *,total_spends/(select sum(total_spends) from cte)*100 as pct
from cte 
limit 5;

-- 2.write a query to print highest spend month and 
-- amount spent in that month for each card type.
with cte as(
select  year(str_to_date(transaction_date,'%d-%b-%y'))as year,month(str_to_date(transaction_date,'%d-%b-%y'))as month,sum(amount)as amounts
from credit_card_transcations
group by 1,2
order by amounts desc
limit 1)
select card_type,sum(amount)as sum
from credit_card_transcations
where (year(str_to_date(transaction_date,'%d-%b-%y')),month(str_to_date(transaction_date,'%d-%b-%y'))) in (select year,month from cte)
group by 1,year(str_to_date(transaction_date,'%d-%b-%y')),
month(str_to_date(transaction_date,'%d-%b-%y'))
;

-- 3. write a query to print the transaction details
-- (all columns from the table) for each card type 
-- when it reaches a cumulative of 1000000 total spends
-- (We should have 4 rows in the o/p one for each card type).


with cte as(
select *,sum(amount)over(partition by card_type order by 
str_to_date(transaction_date,'%d-%b-%y'),transaction_id )as rnning_total
from  credit_card_transcations
),drank as(

select *,dense_rank()over(partition by card_type order by rnning_total desc)as drnk
from cte where rnning_total<=1000000
)

select * from drank
where drnk=1
;



-- 4. write a query to find city which had lowest percentage spend for gold card type.


with cte as(
select city,sum(amount) as total
from credit_card_transcations
where card_type='gold'
group by 1)
select city,total/(select sum(total) from cte)*100 as pct
from cte
group by 1
order by pct
limit 1;



-- 5. write a query to print 3 columns
-- : city, highest_expense_type , lowest_expense_type 
-- (example format : Delhi , bills, Fuel).

with cte as(
select city,exp_type,sum(amount) as sum
from credit_card_transcations
group by 1,2
),
cte2 as(
select city,exp_type,dense_rank()over(partition by
city order by sum desc) as hdrnk,
dense_rank()over(partition by
city order by sum asc) as ldrnk
from cte 
)


select city,
max(case when hdrnk=1 then exp_type else 0 end )as highst,
max(case when ldrnk=1 then exp_type else 0 end )as lowest
 
 from cte2
 group by 1;

-- 6. write a query to find percentage contribution of spends by females for each expense type.


select exp_type,sum(amount)as total,sum(case when gender='f' then amount end )as female_purch,
sum(case when gender='f' then amount end )*100 /sum(amount) as pct
from credit_card_transcations
group by 1;


-- 7. which card and expense type combination saw highest month over month growth in Jan-2014.
with cte as(
select year(str_to_date(transaction_date,'%d-%b-%y'))as years,
month(str_to_date(transaction_date,'%d-%b-%y')) as months,
card_type,exp_type,sum(amount) as amounts
from credit_card_transcations
group by 1,2,card_type,exp_type),
cte2 as(
select *,lag(amounts,1)over(partition by card_type,exp_type order by years,months) as prev_amount
from cte) 
select *,amounts-prev_amount/prev_amount*100 as pct from cte2
where years='2014'
and months='01'


;

-- 8. during weekends which city has highest total spend to total no of transcations ratio.
 
 select city,sum(amount)/count(*) as ratio
from credit_card_transcations
where dayofweek(str_to_date(transaction_date,'%d-%b-%y'))in(1,7)
group by 1
order by 2 desc 
limit 1;


-- 9. which city took least number of days to reach its 
-- 500th transaction after the first transaction in that city.

with cte as(
select city
from credit_card_transcations
group by city
having count(*)>=500),
cte2 as(
select *,row_number ()over(partition by city order by transaction_date,transaction_id)as rn
from credit_card_transcations
where city in(select city from cte)),
cte3 as(
select city,min(str_to_date(transaction_date,'%d-%b-%y')) as first_date,max(str_to_date(transaction_date,'%d-%b-%y')) as last_date
from cte2
where rn<=500
group by 1)
select *,datediff(last_date,first_date)as day_dif
from cte3 
order by day_dif
asc
limit 1;
