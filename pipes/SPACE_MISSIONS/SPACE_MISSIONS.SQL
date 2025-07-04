LOAD DATA LOCAL INFILE '/Users/calebbousman/Documents/Git/Ocean_Studies/sources/space_missions_cleaned.csv'
INTO TABLE PUBLISH_D.SPACE_MISSIONS
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
(mission_date, mission_id, destination, status, crew_size, duration_days, success_rate, security_code);

select * from PUBLISH_D.SPACE_MISSIONS;
truncate table PUBLISH_D.SPACE_MISSIONS;

SELECT 
    MAX(duration_days),
    security_code,
    mission_date
FROM PUBLISH_D.SPACE_MISSIONS
WHERE STATUS = 'Completed'
ORDER BY duration_days DESC
LIMIT 10;

