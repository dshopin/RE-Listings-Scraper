-- Converts longitudinal data from "full" to "incremental".

-- "Full" - all listings downloaded daily and appended to the existing table (daily snapshots)
-- "Incremental"  - after first full download only changes are added.

-- In incremental table each record has StartDate and EndDate. The latest record, describing the current state of some listing,
-- has EndDate=NULL. As soon as something changed, EndDate set to current_date and 
-- new record for this listing appended (with StartDate=current_date and EndDate=NULL).
-- If some listing is not in new data anymore, then its latest record's EndDate set to current_date.
-- If new listing occured, then it's added to the table.

DROP PROCEDURE IF EXISTS `to_incremental`;

DELIMITER $$
USE `realtor`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `to_incremental`()
BEGIN

	DROP TABLE IF EXISTS listings_copy;
    
    CREATE TABLE listings_copy AS
		SELECT * FROM listings;

	UPDATE listings_copy
		SET DownloadDate = STR_TO_DATE(DownloadDate, '%d-%m-%Y');
	
    ALTER TABLE listings_copy CHANGE COLUMN DownloadDate StartDate DATETIME;
	ALTER TABLE listings_copy ADD COLUMN EndDate DATETIME DEFAULT NULL AFTER StartDate;
    
        
	SELECT @firstday := MIN(StartDate) FROM listings_copy;
	SELECT @lastday := MAX(StartDate) FROM listings_copy;	

	CREATE TABLE listings_inc AS
		SELECT * FROM listings_copy
		WHERE StartDate = @firstday; 
    
    SET @currday = DATE_ADD(@firstday, INTERVAL 1 DAY);
	REPEAT
			SELECT @currday;
            
            DROP TEMPORARY TABLE IF EXISTS upd;
            
            CREATE TEMPORARY TABLE upd AS
				SELECT * FROM listings_copy
				WHERE StartDate = @currday;
                
			CALL realtor.add_incremental(@currday);

            
			SET @currday = DATE_ADD(@currday, INTERVAL 1 DAY);
            
            DROP TEMPORARY TABLE IF EXISTS upd;
					
	UNTIL @currday > @lastday END REPEAT;
    
    
	DROP TABLE IF EXISTS listings_copy;

    
END$$
DELIMITER ;

