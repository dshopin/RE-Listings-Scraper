use realtor;

########################################################################################################################
# listings with gaps 
########################################################################################################################
set @rownum = 0;

create table rownums as
	select Property_Address_AddressText, StartDate, EndDate,
		@rownum := if(@prevadd = Property_Address_AddressText, @rownum + 1,1) as rownum,
		@prevadd := Property_Address_AddressText
from (select * from listings_inc
	  order by Property_Address_AddressText, StartDate) t;

select t1.Property_Address_AddressText, t1.StartDate, t1.EndDate, t2.StartDate as StartDateNext, t2.EndDate as EndDateNext
from rownums t1
left join rownums t2
on t1.Property_Address_AddressText=t2.Property_Address_AddressText
	and t1.rownum + 1 = t2.rownum
where t2.StartDate > t1.EndDate;

drop table if exists rownums;
########################################################################################################################



########################################################################################################################
# Daily stats
########################################################################################################################
set @mindate = '2018-12-13';
set @maxdate = current_date();

create table dates as
select * from 
(select adddate(@mindate,t4.i*10000 + t3.i*1000 + t2.i*100 + t1.i*10 + t0.i) selected_date from
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t0,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t1,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t2,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t3,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t4) v
where selected_date <= cast(@maxdate as datetime);


select d.selected_date,
		count(l.Property_Address_AddressText) as active_listings,
        avg(l.Property_Price) as mean_price
from dates d
left join listings_inc l
on d.selected_date >= l.StartDate and (d.selected_date < l.EndDate or l.EndDate is null)
group by d.selected_date;

drop table if exists dates;





select DownloadDate, count(*)
from listings l
left join duplicates d
on l.Property_Address_AddressText=d.Property_Address_AddressText
and STR_TO_DATE(l.DownloadDate, '%d-%m-%Y') = d.StartDate
where d.StartDate is null
group by DownloadDate;
