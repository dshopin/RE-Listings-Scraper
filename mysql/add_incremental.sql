-- Adds new data to "incremental" table.

-- In incremental table each record has StartDate and EndDate. The latest record, describing the current state of some listing,
-- has EndDate=NULL. As soon as something changed, EndDate set to current_date and 
-- new record for this listing appended (with StartDate=current_date and EndDate=NULL).
-- If some listing is not in new data anymore, then its latest record's EndDate set to current_date.
-- If new listing occured, then it's added to the table.

-- Tables `listings_inc` (main table with all previous data) and `upd` (new data) should exist for this procedure to run

DROP PROCEDURE IF EXISTS `add_incremental`;

DELIMITER $$
USE `realtor`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `add_incremental`(IN currdate DATETIME)
BEGIN
	DROP TEMPORARY TABLE IF EXISTS toend;
	DROP TEMPORARY TABLE IF EXISTS toadd;
	DROP TEMPORARY TABLE IF EXISTS toupd;

	# lookup table of listings that are not in the new data
	CREATE TEMPORARY TABLE toend AS
		SELECT t.Property_Address_AddressText, t.Id, t.MlsNumber
		FROM listings_inc t
		LEFT JOIN upd
		ON t.Property_Address_AddressText = upd.Property_Address_AddressText
		AND t.Id = upd.Id AND t.MlsNumber = upd.MlsNumber
		WHERE upd.Id IS NULL AND t.EndDate IS NULL;
	


	#lookup table of new records
	CREATE TEMPORARY TABLE toadd AS
		SELECT	upd.Property_Address_AddressText, upd.Id, upd.MlsNumber
		FROM listings_inc t
		RIGHT JOIN upd
		ON t.Property_Address_AddressText = upd.Property_Address_AddressText
		AND t.Id = upd.Id AND t.MlsNumber = upd.MlsNumber
		WHERE t.Id IS NULL AND t.EndDate IS NULL;
        



	# lookup table of listings to update
	CREATE TEMPORARY TABLE toupd AS
		SELECT	upd.Property_Address_AddressText, upd.Id, upd.MlsNumber
		FROM listings_inc t
		INNER JOIN upd
		ON t.Property_Address_AddressText = upd.Property_Address_AddressText
		AND t.Id = upd.Id AND t.MlsNumber = upd.MlsNumber
		WHERE t.EndDate IS NULL AND
			  (t.Building_BathroomTotal <> upd.Building_BathroomTotal OR
			  t.Building_Bedrooms <> upd.Building_Bedrooms OR
			  t.Building_SizeInterior <> upd.Building_SizeInterior OR
			  t.Building_Type <> upd.Building_Type OR
			  t.Property_Price <> upd.Property_Price OR
			  t.Property_Type <> upd.Property_Type OR
			  t.Property_Address_Longitude <> upd.Property_Address_Longitude OR
			  t.Property_Address_Latitude <> upd.Property_Address_Latitude OR
			  t.Property_TypeId <> upd.Property_TypeId OR
			  t.Property_OwnershipType <> upd.Property_OwnershipType OR
			  t.Land_SizeTotal <> upd.Land_SizeTotal OR
			  t.RelativeDetailsURL <> upd.RelativeDetailsURL OR
			  t.Building_StoriesTotal <> upd.Building_StoriesTotal);



	# "closing" those records in the table that are not in the update
	UPDATE listings_inc t
		SET EndDate = currdate
		WHERE EXISTS (SELECT 1
					  FROM toend
					  WHERE t.Property_Address_AddressText = toend.Property_Address_AddressText
							AND t.Id = toend.Id AND t.MlsNumber = toend.MlsNumber
							AND t.EndDate IS NULL);

	# adding new records to t
	INSERT INTO listings_inc (`Id`,`MlsNumber`,`Building_BathroomTotal`,`Building_Bedrooms`,`Building_SizeInterior`,
							`Building_Type`,`Property_Price`,`Property_Type`,`Property_Address_AddressText`,
							`Property_Address_Longitude`,`Property_Address_Latitude`,`Property_TypeId`,
							`Property_OwnershipType`,`Land_SizeTotal`,`RelativeDetailsURL`,`Building_StoriesTotal`,
							`StartDate`,`EndDate`,`Building_SizeInterior_SqFt`,`Land_SizeTotal_SqFt`)
		SELECT `Id`,`MlsNumber`,`Building_BathroomTotal`,`Building_Bedrooms`,`Building_SizeInterior`,
							`Building_Type`,`Property_Price`,`Property_Type`,`Property_Address_AddressText`,
							`Property_Address_Longitude`,`Property_Address_Latitude`,`Property_TypeId`,
							`Property_OwnershipType`,`Land_SizeTotal`,`RelativeDetailsURL`,`Building_StoriesTotal`,
							`StartDate`,`EndDate`,`Building_SizeInterior_SqFt`,`Land_SizeTotal_SqFt`
		FROM upd
		WHERE EXISTS (SELECT 1
					  FROM toadd
					  WHERE upd.Property_Address_AddressText = toadd.Property_Address_AddressText
							AND upd.Id = toadd.Id AND upd.MlsNumber = toadd.MlsNumber);


	### updating changed listings ###
	# 1) closing old records
	UPDATE listings_inc t
		SET EndDate = currdate
		WHERE EXISTS (SELECT 1
					  FROM toupd
					  WHERE t.Property_Address_AddressText = toupd.Property_Address_AddressText
							AND t.Id = toupd.Id AND t.MlsNumber = toupd.MlsNumber
							AND t.EndDate IS NULL);                        
	# 2) adding changed records
	INSERT INTO listings_inc (`Id`,`MlsNumber`,`Building_BathroomTotal`,`Building_Bedrooms`,`Building_SizeInterior`,
							`Building_Type`,`Property_Price`,`Property_Type`,`Property_Address_AddressText`,
							`Property_Address_Longitude`,`Property_Address_Latitude`,`Property_TypeId`,
							`Property_OwnershipType`,`Land_SizeTotal`,`RelativeDetailsURL`,`Building_StoriesTotal`,
							`StartDate`,`EndDate`,`Building_SizeInterior_SqFt`,`Land_SizeTotal_SqFt`)
		SELECT `Id`,`MlsNumber`,`Building_BathroomTotal`,`Building_Bedrooms`,`Building_SizeInterior`,
							`Building_Type`,`Property_Price`,`Property_Type`,`Property_Address_AddressText`,
							`Property_Address_Longitude`,`Property_Address_Latitude`,`Property_TypeId`,
							`Property_OwnershipType`,`Land_SizeTotal`,`RelativeDetailsURL`,`Building_StoriesTotal`,
							`StartDate`,`EndDate`,`Building_SizeInterior_SqFt`,`Land_SizeTotal_SqFt`
		FROM upd
		WHERE EXISTS (SELECT 1
					  FROM toupd
					  WHERE upd.Property_Address_AddressText = toupd.Property_Address_AddressText
							AND upd.Id = toupd.Id AND upd.MlsNumber = toupd.MlsNumber);

    DROP TEMPORARY TABLE IF EXISTS toend;
	DROP TEMPORARY TABLE IF EXISTS toadd;
	DROP TEMPORARY TABLE IF EXISTS toupd;
    
END$$
DELIMITER ;

