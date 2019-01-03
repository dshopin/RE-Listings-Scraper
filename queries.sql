select DownloadDate, count(*)
from realtor.listings
group by DownloadDate;

select * from realtor.listings;

select count(*) from realtor.listings;



select Building_Type, Property_Type, Property_TypeId, Property_OwnershipType, count(*)
from realtor.listings
group by Building_Type, Property_Type, Property_TypeId, Property_OwnershipType
order by Building_Type, Property_Type, Property_TypeId, Property_OwnershipType;


select Id, count(distinct MlsNumber) as mls
from realtor.listings
group by Id
order by mls desc;



with multiid as
(select MlsNumber
from realtor.listings
group by MlsNumber
having count(distinct Id) > 1)

select * from realtor.listings
where MlsNumber in (select * from multiid)
order by MlsNumber, DownloadDate;




with multimls as
(select Property_Address_AddressText, count(distinct MlsNumber) as mls
from realtor.listings
group by Property_Address_AddressText
having mls > 1)

select * from realtor.listings
where Property_Address_AddressText in (select Property_Address_AddressText from multimls)
order by Property_Address_AddressText, DownloadDate;



select Property_Address_AddressText, MlsNumber, Id, DownloadDate, count(*) as count
from realtor.listings
group by Property_Address_AddressText, MlsNumber, Id, DownloadDate
order by count desc;



select Property_Address_AddressText, MlsNumber, Id,
		count(*),
		count(distinct Building_BathroomTotal) as c1,
        count(distinct Building_Bedrooms) as c2,
        count(distinct Building_SizeInterior) as c3,
        count(distinct Building_Type) as c4,
        count(distinct Property_Price) as c5,
        count(distinct Property_Type) as c6,
        count(distinct Property_Address_Longitude) as c7,
        count(distinct Property_Address_Latitude) as c8,
        count(distinct Property_TypeId) as c9,
        count(distinct Property_OwnershipType) as c10,
        count(distinct Land_SizeTotal) as c11,
		count(distinct RelativeDetailsURL) as c12,
        count(distinct Building_StoriesTotal) as c13
from realtor.listings
group by Property_Address_AddressText, MlsNumber, Id
having c1 >1 or c2 > 1 or c3 > 1 or c4 > 1 or c5 > 1 or c6 > 1 or c7 > 1 or c8 > 1 or
 c9 > 1 or c10 > 1 or c11 > 1 or c12 > 1 or c13 > 1;
 
 
 # trying longitudinal data update
use realtor;

create table test as select * from listings;
 
create table test_13 as
select * from test
where DownloadDate = '13-12-2018'; 

create table test_15 as
select * from test
where DownloadDate = '15-12-2018';

select * from test_13 limit 10;
select * from test_15 limit 10;

select count(*) from test_13;
select count(*) from test_15;
  
alter table test_13 change column DownloadDate StartDate text;
alter table test_13 add column EndDate text default NULL after StartDate;

alter table test_15 change column DownloadDate StartDate text;
alter table test_15 add column EndDate text default NULL after StartDate;


#active records in the base that not in update
UPDATE test_13 t13
	SET EndDate='15-12-2018'
    WHERE EXISTS (SELECT 1
				  FROM (select base.Property_Address_AddressText, base.Id, base.MlsNumber
						from test_13 base
						left join test_15 upd
						on base.Property_Address_AddressText = upd.Property_Address_AddressText
						and base.Id = upd.Id and base.MlsNumber = upd.MlsNumber
						where upd.Id is NULL and base.EndDate is NULL) t
				WHERE t13.Property_Address_AddressText = t.Property_Address_AddressText
						and t13.Id = t.Id and t13.MlsNumber = t.MlsNumber
					)
;


select * from test_13 where EndDate is not NULL;

#new records in update that are not in active base
select	upd.Property_Address_AddressText, upd.Id, upd.MlsNumber
from test_13 base
right join test_15 upd
on base.Property_Address_AddressText = upd.Property_Address_AddressText
and base.Id = upd.Id and base.MlsNumber = upd.MlsNumber
where base.Id is NULL;


# records in update that exist in active base, but have some differences
select	upd.Property_Address_AddressText, upd.Id, upd.MlsNumber
from test_13 base
inner join test_15 upd
on base.Property_Address_AddressText = upd.Property_Address_AddressText
and base.Id = upd.Id and base.MlsNumber = upd.MlsNumber
where base.Building_BathroomTotal <> upd.Building_BathroomTotal or
      base.Building_Bedrooms <> upd.Building_Bedrooms or
      base.Building_SizeInterior <> upd.Building_SizeInterior or
      base.Building_Type <> upd.Building_Type or
      base.Property_Price <> upd.Property_Price or
      base.Property_Type <> upd.Property_Type or
      base.Property_Address_Longitude <> upd.Property_Address_Longitude or
      base.Property_Address_Latitude <> upd.Property_Address_Latitude or
      base.Property_TypeId <> upd.Property_TypeId or
      base.Property_OwnershipType <> upd.Property_OwnershipType or
      base.Land_SizeTotal <> upd.Land_SizeTotal or
	  base.RelativeDetailsURL <> upd.RelativeDetailsURL or
      base.Building_StoriesTotal <> upd.Building_StoriesTotal
;



select	*
from test_13
where Property_Address_AddressText='325 2451 GLADWIN ROAD|Abbotsford, British Columbia V2T3N8'
and Id ='19996293'  and MlsNumber = 'R2303151';

select	*
from test_15
where Property_Address_AddressText='325 2451 GLADWIN ROAD|Abbotsford, British Columbia V2T3N8'
and Id ='19996293'  and MlsNumber = 'R2303151';


















SELECT * FROM t
WHERE EndDate IS NOT NULL;

SELECT * FROM t
WHERE StartDate = '14-12-2018';


SELECT * FROM t b1
WHERE EXISTS (SELECT 1
				FROM (SELECT Property_Address_AddressText, Id, MlsNumber, COUNT(*)
						FROM t
						GROUP BY Property_Address_AddressText, Id, MlsNumber
						HAVING COUNT(*) >1) b2
				WHERE b1.Property_Address_AddressText=b2.Property_Address_AddressText
                AND b1.Id=b2.Id
                AND b1.MlsNumber=b2.MlsNumber)
ORDER BY Property_Address_AddressText, Id, MlsNumber, StartDate
;





CALL `realtor`.`to_incremental`();

use realtor;

select count(*) from listings_inc;


select Updated, Active, count(*)
from (select Property_Address_AddressText, Id, MlsNumber, count(*) as tot, count(EndDate) as enddates,
	case when count(EndDate) < count(*) then 1 else 0 end as Active,
	case when count(*) > 1 then 1 else 0 end as Updated
	from listings_inc
	group by Property_Address_AddressText, Id, MlsNumber) t
group by Updated, Active
;


create temporary table classes as
	select Property_Address_AddressText, Id, MlsNumber, count(*) as tot, count(EndDate) as enddates,
	case when count(EndDate) < count(*) then 1 else 0 end as Active,
	case when count(*) > 1 then 1 else 0 end as Updated
	from listings_inc
	group by Property_Address_AddressText, Id, MlsNumber;
    
    
## checking those that have only one record (no updates) and still active
select l.* from listings_inc l
inner join classes c	
on l.Property_Address_AddressText=c.Property_Address_AddressText
	AND l.Id=c.Id
	AND l.MlsNumber=c.MlsNumber
	AND c.Active=1 AND c.Updated=0
order by Property_Address_AddressText, Id, MlsNumber;
    
select l.* from listings l
inner join classes c	
on l.Property_Address_AddressText=c.Property_Address_AddressText
	AND l.Id=c.Id
	AND l.MlsNumber=c.MlsNumber
	AND c.Active=1 AND c.Updated=0
order by Property_Address_AddressText, Id, MlsNumber;


select distinct Id, MlsNumber, Building_BathroomTotal, Building_Bedrooms, Building_SizeInterior,
Building_Type, Property_Price, Property_Type, Property_Address_AddressText, Property_Address_Longitude, Property_Address_Latitude,
Property_TypeId, Property_OwnershipType, Land_SizeTotal, RelativeDetailsURL, Building_StoriesTotal,
Building_SizeInterior_SqFt, Land_SizeTotal_SqFt
 from listings
where Id='19761745';




select * from listings_inc where Id='19761745';
select * from listings where Id='19761745';


select * from classes where Updated=1;




select * from upd;
select * from listings_inc;
select count(*) from listings;

call realtor.to_incremental();

select EndDate, count(*)
from listings_inc
group by EndDate;

select StartDate, count(*)
from listings_inc
group by StartDate;


select DownloadDate, count(*)
from listings
group by DownloadDate;

create table listings_inc_copy as select * from listings_inc;
create table upd_copy as select * from upd;

call realtor.add_incremental(CAST(CURDATE() AS DATETIME));

call realtor.dummy(CAST(CURDATE() AS DATETIME));

create table listings_inc as select * from listings_inc_copy;
create table upd as select * from upd_copy;



select * from dummy;
call realtor.dummy(CURDATE());


insert into dummy select curdate();



# new listings
select Property_Address_AddressText, Id, MlsNumber
from listings_inc
group by Property_Address_AddressText, Id, MlsNumber
having max(StartDate) = curdate() and max(EndDate) is Null;

#updated listings
select Property_Address_AddressText, Id, MlsNumber
from listings_inc
group by Property_Address_AddressText, Id, MlsNumber
having max(StartDate) = curdate() and max(EndDate) = curdate();

# revived listings
select Property_Address_AddressText, Id, MlsNumber
from listings_inc
group by Property_Address_AddressText, Id, MlsNumber
having max(StartDate) = curdate() and max(EndDate) < curdate();

# withdrawn listings
select Property_Address_AddressText, Id, MlsNumber
from listings_inc
group by Property_Address_AddressText, Id, MlsNumber
having max(EndDate) = curdate() and max(StartDate) < curdate();






select * from upd
where Property_Address_AddressText='1 10581 140 STREET|Surrey, British Columbia'
and Id='19996652' and MlsNumber='R2306107';