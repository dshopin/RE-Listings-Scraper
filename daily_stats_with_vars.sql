use realtor;

########################################################################################################################
# Daily stats
########################################################################################################################
set @rownum=0;
set @prevadd='';
set @prevgap=0;
set @prevend='';
set @prevprice=0;
set @prevbirth='';

# add a flag of new listing
with rownums as
(
select Property_Address_AddressText, StartDate, EndDate, price,
		@rownum := if(@prevadd = Property_Address_AddressText, @rownum + 1, 1) as rownum,
        if(@prevadd = Property_Address_AddressText, datediff(StartDate, @prevend), null) as gap,
        if(@prevadd = Property_Address_AddressText, (price - @prevprice)/@prevprice, null) as price_change,
        @prevbirth := if(@prevadd = Property_Address_AddressText,
						if(StartDate < @prevbirth, StartDate, @prevbirth),
                        StartDate) as BirthDate,
        @prevadd := Property_Address_AddressText,
        @prevend := EndDate,
        @prevprice := price
from (select *,
	  cast(replace(replace(Property_Price,'$',''),',','') as decimal(10,2)) as price
      from listings_inc
       -- where Property_Address_Latitude between 49.293457 and 49.308352
	   -- and Property_Address_Longitude between -122.767648 and -122.732808
	  order by Property_Address_AddressText, StartDate) t
)
,
# create calendar table
dates as
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

select d.selected_date,
		count(r.Property_Address_AddressText) as active_listings
        ,sum(case when r.gap is null and d.selected_date = r.StartDate then 1 else 0 end) as new_listings
        ,sum(case when r.gap > 0  and d.selected_date = r.StartDate then 1 else 0 end) as relistings
        ,sum(case when r.gap = 0  and d.selected_date = r.StartDate then 1 else 0 end) as upd_listings
        ,avg(case when r.gap > 0  and d.selected_date = r.StartDate then gap else null end) as mean_gap #mean days since end of last listing for those relisted this day
        ,concat('$',format(avg(price),2)) as mean_price
        ,sum(case when r.price_change < 0 and r.price_change is not null  and d.selected_date = r.StartDate then 1 else 0 end) as lowered_price_listings
        ,sum(case when r.price_change > 0 and r.price_change is not null  and d.selected_date = r.StartDate then 1 else 0 end) as upped_price_listings
        ,concat(format(avg(case when r.price_change <> 0 and r.price_change is not null  and d.selected_date = r.StartDate then price_change else null end) * 100,2),'%') as mean_price_change
        ,concat(format(avg(case when r.price_change < 0 and r.price_change is not null  and d.selected_date = r.StartDate then price_change else null end) * 100,2),'%') as mean_lowered_price_change
        ,avg(datediff(d.selected_date, r.BirthDate) + 1)as mean_days_in_market # ignoring any gaps
from dates d
left join rownums r
on d.selected_date >= r.StartDate and (d.selected_date < r.EndDate or r.EndDate is null)
group by d.selected_date
order by d.selected_date;


