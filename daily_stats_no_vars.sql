use realtor;


with precalc as
(
select *,
		cast(replace(replace(Property_Price,'$',''),',','') as decimal(10,2)) as price,
        datediff(if(EndDate is null, adddate(curdate(), interval 1 day), EndDate), StartDate) as duration
from listings_inc
order by Property_Address_AddressText, StartDate
)
,
rownums as
(
-- select t1.Property_Address_AddressText, t1.StartDate, count(*) as rownum
select t1.`Id`,
  t1.`MlsNumber`,
  t1.`Building_BathroomTotal`,
  t1.`Building_Bedrooms`,
  t1.`Building_SizeInterior`,
  t1.`Building_Type`,
  t1.`Property_Price`,
  t1.`Property_Type`,
  t1.`Property_Address_AddressText`,
  t1.`Property_Address_Longitude`,
  t1.`Property_Address_Latitude`,
  t1.`Property_TypeId`,
  t1.`Property_OwnershipType`,
  t1.`Land_SizeTotal`,
  t1.`RelativeDetailsURL`,
  t1.`Building_StoriesTotal`,
  t1.`StartDate`,
  t1.`EndDate`,
  t1.`Building_SizeInterior_SqFt`,
  t1.`Land_SizeTotal_SqFt`,
  -- t1.price,
  -- t1.duration,
  count(*) as rownum,
  coalesce(min(t2.StartDate), t1.StartDate) as BirthDate
from listings_inc t1
left join listings_inc t2
	on t1.Property_Address_AddressText = t2.Property_Address_AddressText
	and t1.StartDate >= t2.StartDate
-- group by t1.Property_Address_AddressText, t1.StartDate
group by t1.`Id`,
  t1.`MlsNumber`,
  t1.`Building_BathroomTotal`,
  t1.`Building_Bedrooms`,
  t1.`Building_SizeInterior`,
  t1.`Building_Type`,
  t1.`Property_Price`,
  t1.`Property_Type`,
  t1.`Property_Address_AddressText`,
  t1.`Property_Address_Longitude`,
  t1.`Property_Address_Latitude`,
  t1.`Property_TypeId`,
  t1.`Property_OwnershipType`,
  t1.`Land_SizeTotal`,
  t1.`RelativeDetailsURL`,
  t1.`Building_StoriesTotal`,
  t1.`StartDate`,
  t1.`EndDate`,
  t1.`Building_SizeInterior_SqFt`,
  t1.`Land_SizeTotal_SqFt`
  -- t1.price,
  -- t1.duration
order by t1.Property_Address_AddressText, rownum

)
, numbered as
(
select l.*, r.rownum
from precalc l
inner join rownums r
	on l.Property_Address_AddressText = r.Property_Address_AddressText
    and l.StartDate = r.StartDate
-- order by l.Property_Address_AddressText, l.StartDate
)

, changes as
(
select n1.*,
	   n2.StartDate as PrevStartDate,
       n2.EndDate as PrevEndDate,
       -- n2.price as PrevPrice,
       datediff(n1.StartDate,n2.EndDate) as gap
       -- (n1.price - n2.price)/n2.price as price_change
       
from rownums n1
left join rownums n2
	on n1.Property_Address_AddressText = n2.Property_Address_AddressText
    and n1.rownum - 1 = n2.rownum
)

,

birth as
(
select Property_Address_AddressText, min(StartDate) as BirthDate
from listings_inc
group by Property_Address_AddressText
-- order by Property_Address_AddressText
)

select d.selected_date,
		count(r.Property_Address_AddressText) as active_listings
        ,sum(case when r.gap is null and d.selected_date = r.StartDate then 1 else 0 end) as new_listings
        ,sum(case when r.gap > 0  and d.selected_date = r.StartDate then 1 else 0 end) as relistings
        ,sum(case when r.gap = 0  and d.selected_date = r.StartDate then 1 else 0 end) as upd_listings
        ,avg(case when r.gap > 0  and d.selected_date = r.StartDate then gap else null end) as mean_gap #mean days since end of last listing for those relisted this day
        -- ,concat('$',format(avg(price),2)) as mean_price
        -- ,sum(case when r.price_change < 0 and r.price_change is not null and d.selected_date = r.StartDate then 1 else 0 end) as lowered_price_listings
       --  ,sum(case when r.price_change > 0 and r.price_change is not null and d.selected_date = r.StartDate then 1 else 0 end) as upped_price_listings
       --  ,concat(format(avg(case when r.price_change <> 0 and r.price_change is not null and d.selected_date = r.StartDate then price_change else null end) * 100,2),'%') as mean_price_change
        -- ,concat(format(avg(case when r.price_change < 0 and r.price_change is not null and d.selected_date = r.StartDate then price_change else null end) * 100,2),'%') as mean_lowered_price_change
        ,avg(datediff(d.selected_date, r.BirthDate) + 1) as mean_days_in_market # ignoring any gaps
from calendar d
inner join changes r
on d.selected_date >= r.StartDate and (d.selected_date < r.EndDate or r.EndDate is null)
group by d.selected_date
;