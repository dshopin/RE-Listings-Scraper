CREATE DATABASE  IF NOT EXISTS `realtor` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */;
USE `realtor`;
-- MySQL dump 10.13  Distrib 8.0.15, for Win64 (x86_64)
--
-- Host: localhost    Database: realtor
-- ------------------------------------------------------
-- Server version	8.0.15

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
 SET NAMES utf8 ;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Dumping routines for database 'realtor'
--
/*!50003 DROP PROCEDURE IF EXISTS `add_incremental` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `add_incremental`(IN currdate DATETIME)
BEGIN
	DROP TEMPORARY TABLE IF EXISTS toend;
	DROP TEMPORARY TABLE IF EXISTS toadd;
	DROP TEMPORARY TABLE IF EXISTS toupd;

	# lookup table of listings that are not in the new data
	CREATE TEMPORARY TABLE toend AS
		SELECT t.Property_Address_AddressText
		FROM listings_inc t
		LEFT JOIN upd
		ON t.Property_Address_AddressText = upd.Property_Address_AddressText
		WHERE upd.Id IS NULL AND t.EndDate IS NULL;
	
    CREATE INDEX address ON toend (Property_Address_AddressText(10));

	#lookup table of new records
	CREATE TEMPORARY TABLE toadd AS
		SELECT	upd.Property_Address_AddressText
		FROM listings_inc t
		RIGHT JOIN upd
		ON t.Property_Address_AddressText = upd.Property_Address_AddressText
			AND t.EndDate IS NULL
		WHERE t.Id IS NULL;
        
	CREATE INDEX address ON toadd (Property_Address_AddressText(10));
        
	# lookup table of listings to update
	CREATE TEMPORARY TABLE toupd AS
		SELECT	upd.Property_Address_AddressText
		FROM listings_inc t
		INNER JOIN upd
		ON t.Property_Address_AddressText = upd.Property_Address_AddressText
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
			  t.Building_StoriesTotal <> upd.Building_StoriesTotal OR
              t.Id <> upd.Id OR
              t.MlsNumber <> upd.MlsNumber);
              
	CREATE INDEX address ON toupd (Property_Address_AddressText(10));



	# "closing" those records in the table that are not in the update
	UPDATE listings_inc t
		SET EndDate = currdate
		WHERE EXISTS (SELECT 1
					  FROM toend
					  WHERE t.Property_Address_AddressText = toend.Property_Address_AddressText
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
					  WHERE upd.Property_Address_AddressText = toadd.Property_Address_AddressText);


	### updating changed listings ###
	# 1) closing old records
	UPDATE listings_inc t
		SET EndDate = currdate
		WHERE EXISTS (SELECT 1
					  FROM toupd
					  WHERE t.Property_Address_AddressText = toupd.Property_Address_AddressText
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
					  WHERE upd.Property_Address_AddressText = toupd.Property_Address_AddressText);

    DROP TEMPORARY TABLE IF EXISTS toend;
	DROP TEMPORARY TABLE IF EXISTS toadd;
	DROP TEMPORARY TABLE IF EXISTS toupd;
    
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `to_incremental` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `to_incremental`()
BEGIN

	DROP TABLE IF EXISTS listings_copy;
    
    CREATE TABLE listings_copy AS
		SELECT * FROM listings;

	UPDATE listings_copy
		SET DownloadDate = STR_TO_DATE(DownloadDate, '%d-%m-%Y');
	
    ALTER TABLE listings_copy CHANGE COLUMN DownloadDate StartDate DATETIME;
	ALTER TABLE listings_copy ADD COLUMN EndDate DATETIME DEFAULT NULL AFTER StartDate;
    
    
    
    # separate addresses that have multiple records for the same date (just 3 exist so far)
    insert into duplicates(Property_Address_AddressText, StartDate, `count(*)`)
	select Property_Address_AddressText, StartDate, count(*)
	from listings_copy
	group by Property_Address_AddressText, StartDate
	having count(*) > 1;
    
    delete from listings_copy
	where  Property_Address_AddressText in
		(select distinct Property_Address_AddressText
		from 
			(select Property_Address_AddressText
			from listings_copy
			group by Property_Address_AddressText, StartDate
			having count(*) > 1) t);
            
            
    
        
	SELECT @firstday := MIN(StartDate) FROM listings_copy;
	SELECT @lastday := MAX(StartDate) FROM listings_copy;	

	CREATE TABLE listings_inc AS
		SELECT * FROM listings_copy
		WHERE StartDate = @firstday;
        
	CREATE INDEX address ON listings_inc (Property_Address_AddressText(10));
    
    SET @currday = DATE_ADD(@firstday, INTERVAL 1 DAY);
	REPEAT
			SELECT @currday;
            
            DROP TEMPORARY TABLE IF EXISTS upd;
            
            CREATE TEMPORARY TABLE upd AS
				SELECT * FROM listings_copy
				WHERE StartDate = @currday;
                
			CREATE INDEX address ON upd (Property_Address_AddressText(10));
                
			CALL realtor.add_incremental(@currday);

            
			SET @currday = DATE_ADD(@currday, INTERVAL 1 DAY);
            
            DROP TEMPORARY TABLE IF EXISTS upd;
					
	UNTIL @currday > @lastday END REPEAT;
    
    
	DROP TABLE IF EXISTS listings_copy;

    
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-10-01 10:21:32
