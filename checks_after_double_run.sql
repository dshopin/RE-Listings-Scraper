use realtor;

select DownloadDate, count(*)
from listings
group by DownloadDate;


select *
from listings_inc_bkp
where StartDate = EndDate and StartDate = '2019-02-14';

select Property_Address_AddressText, count(*)
from listings_inc
where StartDate = '2019-02-14'
group by Property_Address_AddressText
having count(*) >1;



create table listings_inc_bkp as
select * from listings_inc;

delete from listings_inc
where (Property_Address_AddressText='1630 OCEAN PARK ROAD|Surrey, British Columbia V4A3L9' and Building_Bedrooms='6') or
 (Property_Address_AddressText='508 8067 207 STREET|Langley, British Columbia V2Y0N9' and Building_Bedrooms='1');


delete from listings_inc
where (Property_Address_AddressText='7061 144A STREET|Surrey, British Columbia V3S2L2' and StartDate='2019-02-14');