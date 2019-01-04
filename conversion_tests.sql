# Testing if incremental table is correct

use realtor;

select count(*) from listings_inc;

### Internal consistency ###

-- no empty StartDates
select Property_Address_AddressText, count(StartDate), count(*)
from listings_inc
group by Property_Address_AddressText
having count(StartDate) < count(*);

-- only unique StartDates
select Property_Address_AddressText, StartDate, count(*)
from listings_inc
group by Property_Address_AddressText, StartDate
having count(*) > 1;

-- select * from listings
-- where Property_Address_AddressText = '1373-1375 MAPLE STREET|Vancouver, British Columbia V6J3S1'
-- order by DownloadDate;

-- only 0 or 1 records with empty EndDate
select Property_Address_AddressText, count(*)
from listings_inc
where EndDate is NULL
group by Property_Address_AddressText
having count(*) > 1;

-- all EndDate > StartDate or EndDate is NULL
select *
from listings_inc
where EndDate <= StartDate;

-- no overlapping periods
select t1.*
from listings_inc t1
inner join listings_inc t2
on  t1.Property_Address_AddressText=t2.Property_Address_AddressText
	and t1.StartDate <> t2.StartDate
    and t2.StartDate < t1.EndDate and t2.EndDate > t1.StartDate
order by Property_Address_AddressText;


-- select * from listings_inc
-- where Property_Address_AddressText = '1 20959 SAKWI CREEK ROAD|Agassiz, British Columbia V0M1A1'
-- 	  and Id = '20178975'
--       and MlsNumber = 'R2310158';


### Consistency with original full table ### 

-- all listings from full are in incremental, except those in `duplicates`
select f.*
from (select distinct Property_Address_AddressText from listings) f
left join (select distinct Property_Address_AddressText from listings_inc) i
on i.Property_Address_AddressText=f.Property_Address_AddressText
where i.Property_Address_AddressText is NULL;


-- Those that are active in incremental, should have the latest date in the snapshot table
select @maxdate := max(str_to_date(DownloadDate, '%d-%m-%Y')) from listings;

select i.*, f.DownloadDate
from listings_inc i
left join (select Property_Address_AddressText, Id, MlsNumber, DownloadDate
		   from listings
           where str_to_date(DownloadDate, '%d-%m-%Y') = @maxdate) f
on i.Property_Address_AddressText=f.Property_Address_AddressText
	and i.Id = f.Id
    and i.MlsNumber = f.MlsNumber
where i.EndDate is NULL and f.DownloadDate is NULL;



-- listings with gaps should not have all possible DownloadDates in the full table
set @rownum = 0;
create table rownums as
	select Property_Address_AddressText, Id, MlsNumber, StartDate, EndDate,
		@rownum := if(@prevadd = Property_Address_AddressText
					  and @previd = Id
                      and @prevmls = MlsNumber, @rownum + 1,1) as rownum,
		@prevadd := Property_Address_AddressText,
		@previd := Id,
        @prevmls := MlsNumber
from (select * from listings_inc
	  order by  Property_Address_AddressText, Id, MlsNumber, StartDate) t;

select i.*, f.dates
from # listings with gaps from incremental
	(select t1.Property_Address_AddressText, t1.Id, t1.MlsNumber, t1.StartDate, t1.EndDate, t2.StartDate as StartDateNext, t2.EndDate as EndDateNext
	from rownums t1
	left join rownums t2
	on t1.Property_Address_AddressText=t2.Property_Address_AddressText
		and t1.Id = t2.Id
		and t1.MlsNumber = t2.MlsNumber
		and t1.rownum + 1= t2.rownum
	where t2.StartDate > t1.EndDate
    ) i
inner join #how many unique dates they have in the orig. full table
	(select Property_Address_AddressText, Id, MlsNumber, count(distinct DownloadDate) as dates
	from listings
    group by Property_Address_AddressText, Id, MlsNumber) f
on i.Property_Address_AddressText=f.Property_Address_AddressText
	and i.Id = f.Id
    and i.MlsNumber = f.MlsNumber
where f.dates = (select max(str_to_date(DownloadDate, '%d-%m-%Y')) - min(str_to_date(DownloadDate, '%d-%m-%Y')) + 1 from listings)
order by i.Property_Address_AddressText, i.Id, i.MlsNumber, i.StartDate
;
    
drop table if exists rownums;


-- Each listing should have the same number of active days in both tables
select i.*, s.days_snap
from
(select Property_Address_AddressText,
		sum(datediff(if(EndDate is null, adddate(curdate(), interval 1 day) , EndDate), StartDate)) as days_inc
from listings_inc
group by Property_Address_AddressText) i
inner join
(select Property_Address_AddressText, count(*) as days_snap
from listings
group by Property_Address_AddressText) s
on i.Property_Address_AddressText = s.Property_Address_AddressText
having days_inc <> days_snap
;








