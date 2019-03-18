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
counts as
(
select d.selected_date
		,sum(case when l.Building_Type='Apartment' then 1 else 0 end) as Apartment
        ,sum(case when l.Building_Type='House' then 1 else 0 end) as House
        ,sum(case when l.Building_Type='Row / Townhouse' then 1 else 0 end) as Townhouse
        ,sum(case when l.Building_Type='Duplex' then 1 else 0 end) as Duplex
from dates d
left join  listings_inc l
on d.selected_date >= l.StartDate and (d.selected_date < l.EndDate or l.EndDate is null)
group by d.selected_date
)
select *
		,format(Apartment/(Apartment+House+Townhouse+Duplex),2) as ApartmentPct
        ,format(House/(Apartment+House+Townhouse+Duplex),2) as HousePct
        ,format(Townhouse/(Apartment+House+Townhouse+Duplex),2) as TownhousePct
        ,format(Duplex/(Apartment+House+Townhouse+Duplex),2) as DuplexPct
from counts
order by selected_date
;