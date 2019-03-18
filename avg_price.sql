with dates as
(
select * from 
(select adddate('2018-12-13',t4.i*10000 + t3.i*1000 + t2.i*100 + t1.i*10 + t0.i) selected_date from
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t0,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t1,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t2,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t3,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t4) v
where selected_date <= cast(current_date() as datetime)
)
,
cast_price as
(
select StartDate, EndDate, Building_Type, cast(replace(replace(Property_Price,'$',''),',','') as decimal(10,2)) as price
from listings_inc
)

select d.selected_date
		,concat('$',format(avg(case when l.Building_Type='Apartment' then price else NULL end),2)) as Apartment
        ,concat('$',format(avg(case when l.Building_Type='House' then price else NULL end),2)) as House
        ,concat('$',format(avg(case when l.Building_Type='Row / Townhouse' then price else NULL end),2)) as Townhouse
        ,concat('$',format(avg(case when l.Building_Type='Duplex' then price else NULL end),2)) as Duplex
from dates d
left join  cast_price l
on d.selected_date >= l.StartDate and (d.selected_date < l.EndDate or l.EndDate is null)
group by d.selected_date
order by selected_date
;